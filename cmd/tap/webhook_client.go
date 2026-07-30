package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

const (
	maxWebhookEventBytes  = 3 << 20
	deadLetterOutcome     = "dead-letter"
	payloadTooLargeReason = "payload-too-large"
)

type webhookResult struct {
	deadLetter bool
	reason     string
	status     int
}

type WebhookClient struct {
	logger        *slog.Logger
	webhookURL    string
	adminPassword string
	httpClient    *http.Client
}

func (w *WebhookClient) Send(ctx context.Context, evt *OutboxEvt, ackFn func(*OutboxEvt), deadLetterFn func(context.Context, *OutboxEvt, string, int, int) error) {
	if len(evt.Event) > maxWebhookEventBytes {
		w.persistDeadLetter(ctx, evt, payloadTooLargeReason, 0, 0, deadLetterFn)
		return
	}

	retries := 0
	for {
		result, err := w.post(ctx, evt)
		attempts := retries + 1
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Warn("webhook failed, retrying", "error", err, "id", evt.ID, "retries", retries)
			if !waitForWebhookRetry(ctx, retries) {
				return
			}
			retries++
			continue
		}
		if result.deadLetter {
			w.persistDeadLetter(ctx, evt, result.reason, result.status, attempts, deadLetterFn)
			return
		}

		ackFn(evt)
		return
	}
}

func (w *WebhookClient) persistDeadLetter(ctx context.Context, evt *OutboxEvt, reason string, status, attempts int, deadLetterFn func(context.Context, *OutboxEvt, string, int, int) error) {
	retries := 0
	for {
		if err := deadLetterFn(ctx, evt, reason, status, attempts); err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, errDeadLetterNotFound) || errors.Is(err, errDeadLetterConflict) || errors.Is(err, errActiveEventConflict) {
				w.logger.Error("permanent webhook dead-letter conflict", "error", err, "id", evt.ID)
				return
			}
			w.logger.Error("failed to persist webhook dead letter, retrying", "error", err, "id", evt.ID, "retries", retries)
			if !waitForWebhookRetry(ctx, retries) {
				return
			}
			retries++
			continue
		}
		return
	}
}

func waitForWebhookRetry(ctx context.Context, retries int) bool {
	timer := time.NewTimer(backoff(retries, 10))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (w *WebhookClient) post(ctx context.Context, evt *OutboxEvt) (webhookResult, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", w.webhookURL, bytes.NewReader(evt.Event))
	if err != nil {
		return webhookResult{}, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent())
	if w.adminPassword != "" {
		req.SetBasicAuth("admin", w.adminPassword)
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		webhookRequests.WithLabelValues("error").Inc()
		return webhookResult{}, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusRequestEntityTooLarge &&
		hasExactHeader(resp.Header, "x-worklyn-tap-outcome", deadLetterOutcome) &&
		hasExactHeader(resp.Header, "x-worklyn-tap-reason", payloadTooLargeReason) {
		webhookRequests.WithLabelValues("dead_letter").Inc()
		return webhookResult{deadLetter: true, reason: payloadTooLargeReason, status: resp.StatusCode}, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		webhookRequests.WithLabelValues("non_2xx").Inc()
		return webhookResult{}, fmt.Errorf("webhook returned non-2xx status: %d", resp.StatusCode)
	}

	webhookRequests.WithLabelValues("success").Inc()
	return webhookResult{}, nil
}

func hasExactHeader(header http.Header, name, value string) bool {
	values := header.Values(name)
	return len(values) == 1 && values[0] == value
}
