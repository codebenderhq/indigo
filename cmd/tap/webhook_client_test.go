package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebhookTerminalResponseClassification(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		outcome     string
		reason      string
		wantDead    bool
		wantSuccess bool
	}{
		{name: "marked 413", status: 413, outcome: deadLetterOutcome, reason: payloadTooLargeReason, wantDead: true},
		{name: "unmarked 413", status: 413},
		{name: "wrong outcome", status: 413, outcome: "retry", reason: payloadTooLargeReason},
		{name: "wrong reason", status: 413, outcome: deadLetterOutcome, reason: "other"},
		{name: "marked 401", status: 401, outcome: deadLetterOutcome, reason: payloadTooLargeReason},
		{name: "401", status: 401},
		{name: "403", status: 403},
		{name: "408", status: 408},
		{name: "429", status: 429},
		{name: "500", status: 500},
		{name: "success", status: 204, wantSuccess: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if tt.outcome != "" {
					w.Header().Set("x-worklyn-tap-outcome", tt.outcome)
				}
				if tt.reason != "" {
					w.Header().Set("x-worklyn-tap-reason", tt.reason)
				}
				w.WriteHeader(tt.status)
			}))
			defer server.Close()

			client := &WebhookClient{
				logger:     slog.Default(),
				webhookURL: server.URL,
				httpClient: &http.Client{Timeout: time.Second},
			}
			result, err := client.post(t.Context(), &OutboxEvt{ID: 1, Event: []byte(`{"id":1}`)})
			if tt.wantDead {
				require.NoError(t, err)
				assert.True(t, result.deadLetter)
				assert.Equal(t, payloadTooLargeReason, result.reason)
				assert.Equal(t, 413, result.status)
			} else if tt.wantSuccess {
				require.NoError(t, err)
				assert.False(t, result.deadLetter)
			} else {
				require.Error(t, err)
				assert.False(t, result.deadLetter)
			}
		})
	}
}

func TestWebhookDuplicateMarkerHeadersAreNotTerminal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Add("x-worklyn-tap-outcome", deadLetterOutcome)
		w.Header().Add("x-worklyn-tap-outcome", deadLetterOutcome)
		w.Header().Set("x-worklyn-tap-reason", payloadTooLargeReason)
		w.WriteHeader(http.StatusRequestEntityTooLarge)
	}))
	defer server.Close()
	client := &WebhookClient{logger: slog.Default(), webhookURL: server.URL, httpClient: &http.Client{Timeout: time.Second}}

	result, err := client.post(t.Context(), &OutboxEvt{ID: 1, Event: []byte(`{"id":1}`)})
	require.Error(t, err)
	assert.False(t, result.deadLetter)
}

func TestWebhookNetworkFailureIsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := server.URL
	server.Close()
	client := &WebhookClient{
		logger:     slog.Default(),
		webhookURL: url,
		httpClient: &http.Client{Timeout: 100 * time.Millisecond},
	}

	result, err := client.post(context.Background(), &OutboxEvt{ID: 1, Event: []byte(`{"id":1}`)})
	require.Error(t, err)
	assert.False(t, result.deadLetter)
}

func TestWebhookSendsEnvelopeAtThreeMiBLimit(t *testing.T) {
	var received int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		received = len(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client := &WebhookClient{logger: slog.Default(), webhookURL: server.URL, httpClient: &http.Client{Timeout: time.Second}}
	body := make([]byte, maxWebhookEventBytes)
	body[0] = '{'
	body[len(body)-1] = '}'
	for i := 1; i < len(body)-1; i++ {
		body[i] = ' '
	}

	var acked bool
	client.Send(t.Context(), &OutboxEvt{ID: 1, Event: body, Generation: 1}, func(*OutboxEvt) {
		acked = true
	}, func(context.Context, *OutboxEvt, string, int, int) error {
		t.Fatal("three MiB envelope was dead-lettered")
		return nil
	})
	assert.True(t, acked)
	assert.Equal(t, maxWebhookEventBytes, received)
}

func TestWebhookDoesNotRetryPermanentDeadLetterConflict(t *testing.T) {
	client := &WebhookClient{logger: slog.Default()}
	evt := &OutboxEvt{ID: 1, Did: "did:example:conflict", Event: []byte(`{"id":1}`), Generation: 1}
	calls := 0
	client.persistDeadLetter(t.Context(), evt, payloadTooLargeReason, 413, 1, func(context.Context, *OutboxEvt, string, int, int) error {
		calls++
		return errDeadLetterConflict
	})
	assert.Equal(t, 1, calls)
}
