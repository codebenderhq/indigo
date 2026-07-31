package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/cmd/tap/models"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestWebhookMarkedPayloadTooLargeDeadLettersExactlyOnceAndUnblocksDID(t *testing.T) {
	firstRequest := make(chan struct{})
	releaseFirst := make(chan struct{})
	var once sync.Once
	var receivedMu sync.Mutex
	var receivedIDs []uint

	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		var envelope struct {
			ID uint `json:"id"`
		}
		require.NoError(t, json.Unmarshal(body, &envelope))
		receivedMu.Lock()
		receivedIDs = append(receivedIDs, envelope.ID)
		requestNumber := len(receivedIDs)
		receivedMu.Unlock()

		if requestNumber == 1 {
			once.Do(func() { close(firstRequest) })
			<-releaseFirst
			w.Header().Set("x-worklyn-tap-outcome", deadLetterOutcome)
			w.Header().Set("x-worklyn-tap-reason", payloadTooLargeReason)
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	te := newTestEnv(t, testEnvOpts{outboxMode: OutboxModeWebhook, webhookURL: receiver.URL})
	did := "did:example:dead-letter-ordering"
	firstID := te.pushRecordEvents(did, 1, true)[0]

	select {
	case <-firstRequest:
	case <-time.After(time.Second):
		t.Fatal("first webhook request did not arrive")
	}

	var original models.OutboxBuffer
	require.NoError(t, te.db.First(&original, "id = ?", firstID).Error)
	secondID := te.pushRecordEvents(did, 1, true)[0]
	close(releaseFirst)

	require.Eventually(t, func() bool {
		receivedMu.Lock()
		defer receivedMu.Unlock()
		return len(receivedIDs) == 2
	}, time.Second, 5*time.Millisecond)

	var deadLetters []models.OutboxDeadLetter
	require.Eventually(t, func() bool {
		return te.db.Order("id ASC").Find(&deadLetters).Error == nil && len(deadLetters) == 1
	}, time.Second, 5*time.Millisecond)
	deadLetter := deadLetters[0]
	digest := sha256.Sum256([]byte(original.Data))
	assert.Equal(t, firstID, deadLetter.OriginalEventID)
	assert.Equal(t, original.Did, deadLetter.Did)
	assert.Equal(t, original.Live, deadLetter.Live)
	assert.Equal(t, original.Data, deadLetter.Data)
	assert.Equal(t, fmt.Sprintf("%x", digest), deadLetter.SHA256)
	assert.Equal(t, payloadTooLargeReason, deadLetter.Reason)
	assert.Equal(t, http.StatusRequestEntityTooLarge, deadLetter.HTTPStatus)
	assert.Equal(t, 1, deadLetter.Attempts)

	var firstActive int64
	require.NoError(t, te.db.Model(&models.OutboxBuffer{}).Where("id = ?", firstID).Count(&firstActive).Error)
	assert.Zero(t, firstActive)
	receivedMu.Lock()
	assert.Equal(t, []uint{firstID, secondID}, receivedIDs)
	receivedMu.Unlock()
}

func TestWebhookLocalOversizeDeadLettersWithoutSending(t *testing.T) {
	var requests atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	te := newTestEnv(t, testEnvOpts{outboxMode: OutboxModeWebhook, webhookURL: receiver.URL})
	evt := &RecordEvt{
		Did:        "did:example:local-oversize",
		Rev:        "rev",
		Collection: "app.example.large",
		Rkey:       "one",
		Action:     "create",
		Record:     map[string]interface{}{"value": strings.Repeat("x", maxWebhookEventBytes)},
		Cid:        "cid",
	}
	require.NoError(t, te.events.AddRecordEvents(te.ctx, []*RecordEvt{evt}, true, func(*gorm.DB) error { return nil }))

	var deadLetter models.OutboxDeadLetter
	require.Eventually(t, func() bool {
		return te.db.First(&deadLetter).Error == nil
	}, time.Second, 5*time.Millisecond)
	assert.Zero(t, requests.Load())
	assert.Equal(t, payloadTooLargeReason, deadLetter.Reason)
	assert.Zero(t, deadLetter.HTTPStatus)
	assert.Zero(t, deadLetter.Attempts)
	originalData := deadLetter.Data
	for i := 0; i < 2; i++ {
		_, err := te.outbox.RequeueDeadLetter(te.ctx, deadLetter.ID)
		require.ErrorIs(t, err, errDeadLetterTooLarge)
	}
	var activeCount, deadLetterCount int64
	require.NoError(t, te.db.Model(&models.OutboxBuffer{}).Where("id = ?", deadLetter.OriginalEventID).Count(&activeCount).Error)
	require.NoError(t, te.db.Model(&models.OutboxDeadLetter{}).Where("id = ?", deadLetter.ID).Count(&deadLetterCount).Error)
	assert.Zero(t, activeCount)
	assert.Equal(t, int64(1), deadLetterCount)
	require.NoError(t, te.db.First(&deadLetter, deadLetter.ID).Error)
	assert.Equal(t, originalData, deadLetter.Data)
	assert.Nil(t, deadLetter.RequeuedAt)
}

func TestDeadLetterTransactionFailureDoesNotAcknowledge(t *testing.T) {
	db := newDeadLetterTestDB(t)
	config := &TapConfig{EventCacheSize: 10, OutboxParallelism: 1, DisableAcks: true, RetryTimeout: time.Minute}
	events := newReadyEventManager(db, config)
	require.NoError(t, events.AddIdentityEvent(t.Context(), &IdentityEvt{
		Did:      "did:example:transaction-failure",
		Handle:   "failure.example",
		IsActive: true,
		Status:   models.AccountStatusActive,
	}, func(*gorm.DB) error { return nil }))
	evt, exists := events.GetEvent(1)
	require.True(t, exists)

	outbox := NewOutbox(slog.Default(), events, config)
	worker := &DIDWorker{
		outbox:         outbox,
		ctx:            t.Context(),
		did:            evt.Did,
		notifChan:      make(chan struct{}, 1),
		inFlightSentAt: map[uint]inFlightDelivery{evt.ID: deliveryForEvent(evt)},
	}
	outbox.didWorkers = xsync.NewMap[string, *DIDWorker]()
	outbox.didWorkers.Store(evt.Did, worker)

	require.NoError(t, db.Exec(`CREATE TRIGGER fail_dead_letter BEFORE INSERT ON outbox_dead_letters BEGIN SELECT RAISE(FAIL, 'forced failure'); END`).Error)
	beforePersistenceFailures := testutil.ToFloat64(deadLetterPersistenceFailures.WithLabelValues("database"))
	err := outbox.DeadLetterEvent(t.Context(), evt, payloadTooLargeReason, http.StatusRequestEntityTooLarge, 1)
	require.Error(t, err)
	assert.Equal(t, beforePersistenceFailures+1, testutil.ToFloat64(deadLetterPersistenceFailures.WithLabelValues("database")))

	var activeCount, deadLetterCount int64
	require.NoError(t, db.Model(&models.OutboxBuffer{}).Where("id = ?", evt.ID).Count(&activeCount).Error)
	require.NoError(t, db.Model(&models.OutboxDeadLetter{}).Count(&deadLetterCount).Error)
	assert.Equal(t, int64(1), activeCount)
	assert.Zero(t, deadLetterCount)
	_, exists = events.GetEvent(evt.ID)
	assert.True(t, exists)
	worker.mu.Lock()
	_, inFlight := worker.inFlightSentAt[evt.ID]
	worker.mu.Unlock()
	assert.True(t, inFlight)
}

func TestDeadLetterDuplicateIsIdempotentAndConflictIsPermanent(t *testing.T) {
	db := newDeadLetterTestDB(t)
	config := &TapConfig{EventCacheSize: 10, OutboxParallelism: 1, DisableAcks: true, RetryTimeout: time.Minute}
	events := newReadyEventManager(db, config)
	require.NoError(t, events.AddIdentityEvent(t.Context(), &IdentityEvt{
		Did: "did:example:idempotent", Status: models.AccountStatusActive,
	}, func(*gorm.DB) error { return nil }))
	evt, ok := events.GetEvent(1)
	require.True(t, ok)
	outbox := NewOutbox(slog.Default(), events, config)
	worker := outbox.workerFor(evt.Did)
	worker.inFlightSentAt[evt.ID] = deliveryForEvent(evt)

	require.NoError(t, outbox.DeadLetterEvent(t.Context(), evt, payloadTooLargeReason, 413, 1))
	require.NoError(t, outbox.DeadLetterEvent(t.Context(), evt, payloadTooLargeReason, 413, 2))
	var count int64
	require.NoError(t, db.Model(&models.OutboxDeadLetter{}).Count(&count).Error)
	assert.Equal(t, int64(1), count)

	err := outbox.DeadLetterEvent(t.Context(), evt, "different-terminal-reason", 413, 2)
	require.ErrorIs(t, err, errDeadLetterConflict)
	altered := *evt
	altered.Event = []byte(`{"id":1,"altered":true}`)
	err = outbox.DeadLetterEvent(t.Context(), &altered, payloadTooLargeReason, 413, 2)
	require.ErrorIs(t, err, errDeadLetterConflict)
}

func TestSlowMetricReconciliationNeverBlocksTransition(t *testing.T) {
	db := newDeadLetterTestDB(t)
	config := &TapConfig{EventCacheSize: 10, OutboxParallelism: 1, DisableAcks: true, RetryTimeout: time.Minute}
	events := newReadyEventManager(db, config)
	outbox := NewOutbox(slog.Default(), events, config)
	outbox.metricTimeout = 200 * time.Millisecond
	metricStarted := make(chan struct{})
	releaseMetric := make(chan struct{})
	var once sync.Once
	outbox.metricReconcileHook = func(ctx context.Context) error {
		once.Do(func() { close(metricStarted) })
		select {
		case <-releaseMetric:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	outbox.scheduleDeadLetterMetricsRefresh()
	require.NoError(t, events.AddIdentityEvent(t.Context(), &IdentityEvt{
		Did: "did:example:slow-metrics", Status: models.AccountStatusActive,
	}, func(*gorm.DB) error { return nil }))
	evt, ok := events.GetEvent(1)
	require.True(t, ok)
	<-outbox.outgoing
	started := time.Now()
	require.NoError(t, outbox.DeadLetterEvent(t.Context(), evt, payloadTooLargeReason, 413, 1))
	assert.Less(t, time.Since(started), 100*time.Millisecond)
	select {
	case <-metricStarted:
	case <-time.After(time.Second):
		t.Fatal("metric reconciliation did not start")
	}
	var deadLetter models.OutboxDeadLetter
	require.NoError(t, db.First(&deadLetter).Error)
	started = time.Now()
	_, err := outbox.RequeueDeadLetter(t.Context(), deadLetter.ID)
	require.NoError(t, err)
	assert.Less(t, time.Since(started), 100*time.Millisecond)
	close(releaseMetric)
	waitForMetricReconciliation(t, outbox)
}

func TestDeadLetterTransitionForgetsSeenIdentityAcrossGenerations(t *testing.T) {
	db := newDeadLetterTestDB(t)
	config := &TapConfig{EventCacheSize: 10, OutboxParallelism: 1, DisableAcks: true, RetryTimeout: time.Minute}
	events := newReadyEventManager(db, config)
	outbox := NewOutbox(slog.Default(), events, config)
	require.NoError(t, events.AddIdentityEvent(t.Context(), &IdentityEvt{
		Did: "did:example:seen-cleanup", Status: models.AccountStatusActive,
	}, func(*gorm.DB) error { return nil }))
	first := <-outbox.outgoing
	worker := outbox.workerFor(first.Did)
	firstIdentity := identityForEvent(first)
	worker.mu.Lock()
	_, seen := worker.seen[firstIdentity]
	worker.mu.Unlock()
	require.True(t, seen)
	require.NoError(t, outbox.DeadLetterEvent(t.Context(), first, payloadTooLargeReason, 413, 1))
	worker.mu.Lock()
	_, seen = worker.seen[firstIdentity]
	worker.mu.Unlock()
	assert.False(t, seen)

	var firstDeadLetter models.OutboxDeadLetter
	require.NoError(t, db.Where("original_event_id = ? AND generation = ?", first.ID, first.Generation).First(&firstDeadLetter).Error)
	second, err := outbox.RequeueDeadLetter(t.Context(), firstDeadLetter.ID)
	require.NoError(t, err)
	<-outbox.outgoing
	secondIdentity := identityForEvent(second)
	worker.mu.Lock()
	_, seen = worker.seen[secondIdentity]
	worker.mu.Unlock()
	require.True(t, seen)
	require.NoError(t, outbox.DeadLetterEvent(t.Context(), second, payloadTooLargeReason, 413, 1))
	worker.mu.Lock()
	_, seen = worker.seen[secondIdentity]
	worker.mu.Unlock()
	assert.False(t, seen)
	waitForMetricReconciliation(t, outbox)
}

func TestDeadLetterReconcilesAmbiguousCommittedTransition(t *testing.T) {
	db := newDeadLetterTestDB(t)
	config := &TapConfig{EventCacheSize: 10, OutboxParallelism: 1, DisableAcks: true, RetryTimeout: time.Minute}
	events := newReadyEventManager(db, config)
	require.NoError(t, events.AddIdentityEvent(t.Context(), &IdentityEvt{
		Did: "did:example:ambiguous-dead-letter", Status: models.AccountStatusActive,
	}, func(*gorm.DB) error { return nil }))
	evt, ok := events.GetEvent(1)
	require.True(t, ok)
	outbox := NewOutbox(slog.Default(), events, config)
	worker := outbox.workerFor(evt.Did)
	worker.inFlightSentAt[evt.ID] = deliveryForEvent(evt)
	deadLetter := models.OutboxDeadLetter{
		OriginalEventID: evt.ID,
		Generation:      evt.Generation,
		Did:             evt.Did,
		Live:            evt.Live,
		Data:            string(evt.Event),
		SHA256:          eventSHA256(evt.Event),
		Reason:          payloadTooLargeReason,
		HTTPStatus:      413,
		DeadLetteredAt:  time.Now(),
		Attempts:        1,
	}
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&deadLetter).Error; err != nil {
			return err
		}
		return tx.Where("id = ? AND generation = ?", evt.ID, evt.Generation).Delete(&models.OutboxBuffer{}).Error
	}))

	require.NoError(t, outbox.DeadLetterEvent(t.Context(), evt, payloadTooLargeReason, 413, 2))
	_, exists := events.GetEvent(evt.ID)
	assert.False(t, exists)
	worker.mu.Lock()
	_, inFlight := worker.inFlightSentAt[evt.ID]
	worker.mu.Unlock()
	assert.False(t, inFlight)
	waitForMetricReconciliation(t, outbox)
	assert.Equal(t, float64(1), testutil.ToFloat64(deadLetterDepth))
}

func TestRequeueGenerationRejectsStaleDeadLetterAndAckCallbacks(t *testing.T) {
	db := newDeadLetterTestDB(t)
	config := &TapConfig{EventCacheSize: 10, OutboxParallelism: 1, DisableAcks: true, RetryTimeout: time.Minute}
	events := newReadyEventManager(db, config)
	require.NoError(t, events.AddIdentityEvent(t.Context(), &IdentityEvt{
		Did: "did:example:stale-callback", Status: models.AccountStatusActive,
	}, func(*gorm.DB) error { return nil }))
	oldEvent, ok := events.GetEvent(1)
	require.True(t, ok)
	outbox := NewOutbox(slog.Default(), events, config)
	worker := outbox.workerFor(oldEvent.Did)
	worker.inFlightSentAt[oldEvent.ID] = deliveryForEvent(oldEvent)

	require.NoError(t, outbox.DeadLetterEvent(t.Context(), oldEvent, payloadTooLargeReason, 413, 1))
	var deadLetter models.OutboxDeadLetter
	require.NoError(t, db.First(&deadLetter).Error)
	requeued, err := outbox.RequeueDeadLetter(t.Context(), deadLetter.ID)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		worker.mu.Lock()
		defer worker.mu.Unlock()
		delivery, exists := worker.inFlightSentAt[requeued.ID]
		return exists && delivery.generation == 2
	}, time.Second, 5*time.Millisecond)

	err = outbox.DeadLetterEvent(t.Context(), oldEvent, payloadTooLargeReason, 413, 1)
	require.ErrorIs(t, err, errDeadLetterConflict)
	outbox.AckWebhookEvent(oldEvent)
	require.NoError(t, events.DeleteEvents(t.Context(), []outboxAck{{ID: oldEvent.ID, Generation: oldEvent.Generation}}))

	current, exists := events.GetEvent(requeued.ID)
	require.True(t, exists)
	assert.Equal(t, uint64(2), current.Generation)
	worker.mu.Lock()
	delivery, inFlight := worker.inFlightSentAt[requeued.ID]
	worker.mu.Unlock()
	assert.True(t, inFlight)
	assert.Equal(t, uint64(2), delivery.generation)
}

func TestDuplicateRequeueReconcilesWithoutDuplicateDispatch(t *testing.T) {
	db := newDeadLetterTestDB(t)
	config := &TapConfig{EventCacheSize: 10, OutboxParallelism: 1, DisableAcks: true, RetryTimeout: time.Minute}
	events := newReadyEventManager(db, config)
	require.NoError(t, events.AddIdentityEvent(t.Context(), &IdentityEvt{
		Did: "did:example:duplicate-requeue", Status: models.AccountStatusActive,
	}, func(*gorm.DB) error { return nil }))
	evt, ok := events.GetEvent(1)
	require.True(t, ok)
	outbox := NewOutbox(slog.Default(), events, config)
	worker := outbox.workerFor(evt.Did)
	worker.inFlightSentAt[evt.ID] = deliveryForEvent(evt)
	require.NoError(t, outbox.DeadLetterEvent(t.Context(), evt, payloadTooLargeReason, 413, 1))
	var deadLetter models.OutboxDeadLetter
	require.NoError(t, db.First(&deadLetter).Error)

	first, err := outbox.RequeueDeadLetter(t.Context(), deadLetter.ID)
	require.NoError(t, err)
	second, err := outbox.RequeueDeadLetter(t.Context(), deadLetter.ID)
	require.NoError(t, err)
	assert.Equal(t, first.Generation, second.Generation)
	assert.Equal(t, uint64(2), second.Generation)
	var activeCount int64
	require.NoError(t, db.Model(&models.OutboxBuffer{}).Where("id = ?", evt.ID).Count(&activeCount).Error)
	assert.Equal(t, int64(1), activeCount)
	var active models.OutboxBuffer
	require.NoError(t, db.First(&active, "id = ?", evt.ID).Error)
	assert.Equal(t, string(evt.Event), active.Data)
	require.NoError(t, db.First(&deadLetter, deadLetter.ID).Error)
	assert.NotNil(t, deadLetter.RequeuedAt)
	assert.Empty(t, deadLetter.Data)
	assert.Equal(t, int64(len(evt.Event)), deadLetter.PayloadBytes)
}

func TestAmbiguousSuccessfulRequeueSelfReconcilesCache(t *testing.T) {
	db := newDeadLetterTestDB(t)
	config := &TapConfig{EventCacheSize: 10, OutboxParallelism: 1, DisableAcks: true, RetryTimeout: time.Minute}
	events := newReadyEventManager(db, config)
	outbox := NewOutbox(slog.Default(), events, config)
	require.NoError(t, events.AddIdentityEvent(t.Context(), &IdentityEvt{
		Did: "did:example:ambiguous-requeue", Status: models.AccountStatusActive,
	}, func(*gorm.DB) error { return nil }))
	evt := <-outbox.outgoing
	require.NoError(t, outbox.DeadLetterEvent(t.Context(), evt, payloadTooLargeReason, 413, 1))
	var deadLetter models.OutboxDeadLetter
	require.NoError(t, db.First(&deadLetter).Error)
	var hookCalls atomic.Int32
	events.afterRequeueCommitHook = func() error {
		if hookCalls.Add(1) == 1 {
			return errors.New("ambiguous successful requeue")
		}
		return nil
	}

	requeued, err := outbox.RequeueDeadLetter(t.Context(), deadLetter.ID)
	require.NoError(t, err)
	assert.Equal(t, int32(1), hookCalls.Load())
	cached, exists := events.GetEvent(requeued.ID)
	require.True(t, exists)
	assert.True(t, outboxEventsMatch(cached, requeued))
	select {
	case delivered := <-outbox.outgoing:
		assert.True(t, outboxEventsMatch(delivered, requeued))
	case <-time.After(time.Second):
		t.Fatal("self-reconciled requeue was not dispatched")
	}
	waitForMetricReconciliation(t, outbox)
}

func TestEarlyRequeueWorkerUsesLifetimeContextAndStopsOnRunShutdown(t *testing.T) {
	db := newDeadLetterTestDB(t)
	config := &TapConfig{EventCacheSize: 2, OutboxParallelism: 1, DisableAcks: true, RetryTimeout: time.Minute}
	events := newReadyEventManager(db, config)
	require.NoError(t, events.AddIdentityEvent(t.Context(), &IdentityEvt{
		Did: "did:example:saturated", Status: models.AccountStatusActive,
	}, func(*gorm.DB) error { return nil }))
	evt, ok := events.GetEvent(1)
	require.True(t, ok)
	outbox := NewOutbox(slog.Default(), events, config)
	worker := outbox.workerFor(evt.Did)
	worker.inFlightSentAt[evt.ID] = deliveryForEvent(evt)
	require.NoError(t, outbox.DeadLetterEvent(t.Context(), evt, payloadTooLargeReason, 413, 1))
	var deadLetter models.OutboxDeadLetter
	require.NoError(t, db.First(&deadLetter).Error)
	requeued, err := outbox.RequeueDeadLetter(t.Context(), deadLetter.ID)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), requeued.Generation)
	serviceCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		outbox.Run(serviceCtx)
		close(runDone)
	}()
	cancel()
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("outbox did not stop after service context cancellation")
	}
	require.ErrorIs(t, worker.ctx.Err(), context.Canceled)
	var active models.OutboxBuffer
	require.NoError(t, db.First(&active, "id = ?", evt.ID).Error)
	assert.Equal(t, uint64(2), active.Generation)
}

func TestEarlyRequeueWebhookRequestIsCanceledOnRunShutdown(t *testing.T) {
	requestStarted := make(chan struct{})
	requestCanceled := make(chan struct{})
	db := newDeadLetterTestDB(t)
	data := `{"id":90}`
	deadLetter := models.OutboxDeadLetter{
		OriginalEventID: 90, Generation: 1, Did: "did:example:early-webhook", Data: data,
		SHA256: eventSHA256([]byte(data)), PayloadBytes: int64(len(data)), Reason: payloadTooLargeReason,
		HTTPStatus: 413, DeadLetteredAt: time.Now(),
	}
	require.NoError(t, db.Create(&deadLetter).Error)
	config := &TapConfig{EventCacheSize: 10, OutboxParallelism: 1, WebhookURL: "https://webhook.example", RetryTimeout: time.Minute}
	events := newReadyEventManager(db, config)
	outbox := NewOutbox(slog.Default(), events, config)
	outbox.webhook.httpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		close(requestStarted)
		<-req.Context().Done()
		close(requestCanceled)
		return nil, req.Context().Err()
	})}
	_, err := outbox.RequeueDeadLetter(t.Context(), deadLetter.ID)
	require.NoError(t, err)
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("early requeue webhook did not start")
	}
	serviceCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		outbox.Run(serviceCtx)
		close(runDone)
	}()
	cancel()
	select {
	case <-requestCanceled:
	case <-time.After(time.Second):
		t.Fatal("early requeue webhook was not canceled on shutdown")
	}
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("outbox did not finish shutdown")
	}
}

func TestCanceledRequeueLeavesDeadLetterActive(t *testing.T) {
	db := newDeadLetterTestDB(t)
	config := &TapConfig{EventCacheSize: 10, OutboxParallelism: 1, DisableAcks: true, RetryTimeout: time.Minute}
	events := newReadyEventManager(db, config)
	require.NoError(t, events.AddIdentityEvent(t.Context(), &IdentityEvt{
		Did: "did:example:canceled-requeue", Status: models.AccountStatusActive,
	}, func(*gorm.DB) error { return nil }))
	evt, ok := events.GetEvent(1)
	require.True(t, ok)
	outbox := NewOutbox(slog.Default(), events, config)
	worker := outbox.workerFor(evt.Did)
	worker.inFlightSentAt[evt.ID] = deliveryForEvent(evt)
	require.NoError(t, outbox.DeadLetterEvent(t.Context(), evt, payloadTooLargeReason, 413, 1))
	var deadLetter models.OutboxDeadLetter
	require.NoError(t, db.First(&deadLetter).Error)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := outbox.RequeueDeadLetter(ctx, deadLetter.ID)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, db.First(&deadLetter, deadLetter.ID).Error)
	assert.Nil(t, deadLetter.RequeuedAt)
	var activeCount int64
	require.NoError(t, db.Model(&models.OutboxBuffer{}).Where("id = ?", evt.ID).Count(&activeCount).Error)
	assert.Zero(t, activeCount)
}

func TestRequeueWaitsForCompleteDeadLetterTransition(t *testing.T) {
	db := newDeadLetterTestDB(t)
	config := &TapConfig{EventCacheSize: 10, OutboxParallelism: 1, DisableAcks: true, RetryTimeout: time.Minute}
	events := newReadyEventManager(db, config)
	require.NoError(t, events.AddIdentityEvent(t.Context(), &IdentityEvt{
		Did: "did:example:transition-race", Status: models.AccountStatusActive,
	}, func(*gorm.DB) error { return nil }))
	evt, ok := events.GetEvent(1)
	require.True(t, ok)
	outbox := NewOutbox(slog.Default(), events, config)
	worker := outbox.workerFor(evt.Did)
	worker.inFlightSentAt[evt.ID] = deliveryForEvent(evt)

	insertStarted := make(chan struct{})
	releaseInsert := make(chan struct{})
	require.NoError(t, db.Callback().Create().Before("gorm:create").Register("test:block_dead_letter", func(tx *gorm.DB) {
		if tx.Statement.Schema != nil && tx.Statement.Schema.Name == "OutboxDeadLetter" {
			close(insertStarted)
			<-releaseInsert
		}
	}))
	defer db.Callback().Create().Remove("test:block_dead_letter")
	deadDone := make(chan error, 1)
	go func() {
		deadDone <- outbox.DeadLetterEvent(t.Context(), evt, payloadTooLargeReason, 413, 1)
	}()
	<-insertStarted
	requeueDone := make(chan error, 1)
	go func() {
		_, err := outbox.RequeueDeadLetter(t.Context(), 1)
		requeueDone <- err
	}()
	select {
	case err := <-requeueDone:
		t.Fatalf("requeue escaped transition serialization: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseInsert)
	require.NoError(t, <-deadDone)
	require.NoError(t, <-requeueDone)
	var active models.OutboxBuffer
	require.NoError(t, db.First(&active, "id = ?", evt.ID).Error)
	assert.Equal(t, uint64(2), active.Generation)
}

func TestEventManagerRestartIgnoresDeadLetters(t *testing.T) {
	db := newDeadLetterTestDB(t)
	require.NoError(t, db.Create(&models.OutboxBuffer{ID: 10, Did: "did:example:active", Data: `{"id":10}`, Generation: 1}).Error)
	require.NoError(t, db.Create(&models.OutboxDeadLetter{
		OriginalEventID: 11,
		Generation:      1,
		Did:             "did:example:dead",
		Data:            `{"id":11}`,
		SHA256:          strings.Repeat("0", 64),
		Reason:          payloadTooLargeReason,
		DeadLetteredAt:  time.Now(),
	}).Error)
	now := time.Now()
	require.NoError(t, db.Create(&models.OutboxDeadLetter{
		OriginalEventID:   12,
		Generation:        1,
		Did:               "did:example:restored",
		Data:              `{"id":12}`,
		SHA256:            strings.Repeat("1", 64),
		Reason:            payloadTooLargeReason,
		DeadLetteredAt:    now,
		RequeuedAt:        &now,
		RequeueGeneration: 2,
	}).Error)
	require.NoError(t, db.Create(&models.OutboxBuffer{ID: 12, Did: "did:example:restored", Data: `{"id":12}`, Generation: 2}).Error)

	events := NewEventManager(slog.Default(), db, &TapConfig{EventCacheSize: 10})
	outbox := NewOutbox(slog.Default(), events, &TapConfig{EventCacheSize: 10, OutboxParallelism: 1, DisableAcks: true})
	events.LoadEvents(t.Context())
	_, activeLoaded := events.GetEvent(10)
	_, deadLoaded := events.GetEvent(11)
	restored, restoredLoaded := events.GetEvent(12)
	assert.True(t, activeLoaded)
	assert.False(t, deadLoaded)
	require.True(t, restoredLoaded)
	assert.Equal(t, uint64(2), restored.Generation)
	assert.Equal(t, uint64(12), events.nextID.Load())
	queued := make(map[uint]bool)
	for len(queued) < 2 {
		select {
		case evt := <-outbox.outgoing:
			queued[evt.ID] = true
		case <-time.After(time.Second):
			t.Fatal("loaded active events were not dispatched")
		}
	}
	assert.Equal(t, map[uint]bool{10: true, 12: true}, queued)
}

func TestStockAutoMigrateLeavesAdditiveDeadLetterTable(t *testing.T) {
	db := newDeadLetterTestDB(t)
	row := models.OutboxDeadLetter{
		OriginalEventID: 99,
		Did:             "did:example:rollback",
		Data:            `{"id":99}`,
		SHA256:          strings.Repeat("0", 64),
		Reason:          payloadTooLargeReason,
		DeadLetteredAt:  time.Now(),
	}
	require.NoError(t, db.Create(&row).Error)

	require.NoError(t, db.AutoMigrate(
		&models.Repo{},
		&models.RepoRecord{},
		&baseOutboxBuffer{},
		&models.ResyncBuffer{},
		&models.FirehoseCursor{},
		&models.ListReposCursor{},
		&models.CollectionCursor{},
	))
	assert.True(t, db.Migrator().HasTable(&models.OutboxDeadLetter{}))
	var count int64
	require.NoError(t, db.Model(&models.OutboxDeadLetter{}).Count(&count).Error)
	assert.Equal(t, int64(1), count)
}

type baseOutboxBuffer struct {
	ID   uint   `gorm:"primaryKey"`
	Did  string `gorm:"not null"`
	Live bool   `gorm:"not null"`
	Data string `gorm:"type:text;not null"`
}

type priorOutboxDeadLetter struct {
	ID                uint      `gorm:"primaryKey"`
	OriginalEventID   uint      `gorm:"not null"`
	Generation        uint64    `gorm:"not null"`
	Did               string    `gorm:"not null"`
	Live              bool      `gorm:"not null"`
	Data              string    `gorm:"type:text;not null"`
	SHA256            string    `gorm:"type:char(64);not null"`
	Reason            string    `gorm:"type:text;not null"`
	HTTPStatus        int       `gorm:"not null"`
	DeadLetteredAt    time.Time `gorm:"not null"`
	Attempts          int       `gorm:"not null"`
	RequeuedAt        *time.Time
	RequeueGeneration uint64 `gorm:"not null;default:0"`
}

func (baseOutboxBuffer) TableName() string {
	return "outbox_buffers"
}

func (priorOutboxDeadLetter) TableName() string {
	return "outbox_dead_letters"
}

func TestPopulatedBaseSchemaMigratesWithoutDataLoss(t *testing.T) {
	dbPath := t.TempDir() + "/base.db"
	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Migrator().CreateTable(&baseOutboxBuffer{}))
	require.NoError(t, db.Migrator().CreateTable(&priorOutboxDeadLetter{}))
	base := baseOutboxBuffer{ID: 77, Did: "did:example:base-migration", Live: true, Data: `{"id":77}`}
	require.NoError(t, db.Create(&base).Error)
	priorData := `{"id":78}`
	priorDead := priorOutboxDeadLetter{
		OriginalEventID: 78, Generation: 1, Did: "did:example:prior-dead-letter", Data: priorData,
		SHA256: eventSHA256([]byte(priorData)), Reason: payloadTooLargeReason, DeadLetteredAt: time.Now(),
	}
	require.NoError(t, db.Create(&priorDead).Error)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	migrated, err := SetupDatabase("sqlite://"+dbPath, 1)
	require.NoError(t, err)
	t.Cleanup(func() {
		sqlDB, sqlErr := migrated.DB()
		if sqlErr == nil {
			sqlDB.Close()
		}
	})
	var active models.OutboxBuffer
	require.NoError(t, migrated.First(&active, "id = ?", base.ID).Error)
	assert.Equal(t, base.Data, active.Data)
	assert.Equal(t, uint64(1), active.Generation)
	assert.True(t, migrated.Migrator().HasTable(&models.OutboxDeadLetter{}))
	var migratedDead models.OutboxDeadLetter
	require.NoError(t, migrated.First(&migratedDead, "id = ?", priorDead.ID).Error)
	assert.Equal(t, priorData, migratedDead.Data)
	assert.Equal(t, int64(len(priorData)), migratedDead.PayloadBytes)
}

func TestRequeueDeadLetterRestoresExactEvent(t *testing.T) {
	db := newDeadLetterTestDB(t)
	events := newReadyEventManager(db, &TapConfig{EventCacheSize: 10})
	original := models.OutboxBuffer{
		ID:         42,
		Did:        "did:example:requeue",
		Live:       true,
		Data:       "{ \"id\" : 42, \"exact\" : true }\n",
		Generation: 1,
	}
	require.NoError(t, db.Create(&original).Error)
	evt := &OutboxEvt{ID: original.ID, Did: original.Did, Live: original.Live, Event: []byte(original.Data), Generation: original.Generation}
	events.cache[original.ID] = evt
	transition, err := events.DeadLetterEvent(t.Context(), evt, payloadTooLargeReason, http.StatusRequestEntityTooLarge, 3)
	require.NoError(t, err)

	requeued, err := events.RequeueDeadLetter(t.Context(), transition.deadLetter.ID)
	require.NoError(t, err)
	assert.Equal(t, []byte(original.Data), requeued.event.Event)

	var active models.OutboxBuffer
	require.NoError(t, db.First(&active, "id = ?", original.ID).Error)
	assert.Equal(t, original.ID, active.ID)
	assert.Equal(t, original.Did, active.Did)
	assert.Equal(t, original.Live, active.Live)
	assert.Equal(t, original.Data, active.Data)
	assert.Equal(t, uint64(2), active.Generation)
	var count int64
	require.NoError(t, db.Model(&models.OutboxDeadLetter{}).Where("requeued_at IS NULL").Count(&count).Error)
	assert.Zero(t, count)
	var receipt models.OutboxDeadLetter
	require.NoError(t, db.First(&receipt, transition.deadLetter.ID).Error)
	assert.Empty(t, receipt.Data)
	assert.Equal(t, int64(len(original.Data)), receipt.PayloadBytes)
	assert.Equal(t, eventSHA256([]byte(original.Data)), receipt.SHA256)
	cached, exists := events.GetEvent(original.ID)
	require.True(t, exists)
	assert.Equal(t, []byte(original.Data), cached.Event)
	assert.Equal(t, uint64(2), cached.Generation)
}

func TestRequeueDeadLetterPreservesRowOnActiveIDConflict(t *testing.T) {
	db := newDeadLetterTestDB(t)
	events := newReadyEventManager(db, &TapConfig{EventCacheSize: 10})
	original := models.OutboxBuffer{ID: 7, Did: "did:example:conflict", Data: `{"original":true}`, Generation: 1}
	require.NoError(t, db.Create(&original).Error)
	evt := &OutboxEvt{ID: original.ID, Did: original.Did, Event: []byte(original.Data), Generation: 1}
	events.cache[original.ID] = evt
	transition, err := events.DeadLetterEvent(t.Context(), evt, payloadTooLargeReason, 413, 1)
	require.NoError(t, err)
	require.NoError(t, db.Create(&models.OutboxBuffer{ID: original.ID, Did: original.Did, Data: `{"replacement":true}`, Generation: 2}).Error)
	outbox := NewOutbox(slog.Default(), events, &TapConfig{EventCacheSize: 10, OutboxParallelism: 1, DisableAcks: true})
	beforeFailures := testutil.ToFloat64(deadLetterRequeueFailures.WithLabelValues("conflict"))

	_, err = outbox.RequeueDeadLetter(t.Context(), transition.deadLetter.ID)
	require.ErrorIs(t, err, errActiveEventConflict)
	assert.Equal(t, beforeFailures+1, testutil.ToFloat64(deadLetterRequeueFailures.WithLabelValues("conflict")))
	var stillDead models.OutboxDeadLetter
	require.NoError(t, db.First(&stillDead, transition.deadLetter.ID).Error)
	assert.Equal(t, original.Data, stillDead.Data)
}

func TestRequeueRejectsDeadLetterDigestMismatch(t *testing.T) {
	db := newDeadLetterTestDB(t)
	events := newReadyEventManager(db, &TapConfig{EventCacheSize: 10})
	deadLetter := models.OutboxDeadLetter{
		OriginalEventID: 70,
		Generation:      1,
		Did:             "did:example:bad-digest",
		Data:            `{"id":70}`,
		SHA256:          strings.Repeat("0", 64),
		Reason:          payloadTooLargeReason,
		DeadLetteredAt:  time.Now(),
	}
	require.NoError(t, db.Create(&deadLetter).Error)
	outbox := NewOutbox(slog.Default(), events, &TapConfig{EventCacheSize: 10, OutboxParallelism: 1, DisableAcks: true})

	_, err := outbox.RequeueDeadLetter(t.Context(), deadLetter.ID)
	require.ErrorIs(t, err, errActiveEventConflict)
	var activeCount int64
	require.NoError(t, db.Model(&models.OutboxBuffer{}).Where("id = ?", deadLetter.OriginalEventID).Count(&activeCount).Error)
	assert.Zero(t, activeCount)
}

func TestDeadLetterAdminRoutesRequireConfiguredAuthenticationAndRequeue(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{outboxMode: OutboxModeFireAndForget, adminPassword: "secret"})
	active := models.OutboxBuffer{ID: 88, Did: "did:example:admin-requeue", Data: `{"id":88}`, Generation: 1}
	require.NoError(t, te.db.Create(&active).Error)
	te.events.cacheLk.Lock()
	evt := &OutboxEvt{ID: active.ID, Did: active.Did, Event: []byte(active.Data), Generation: 1}
	te.events.cache[active.ID] = evt
	te.events.cacheLk.Unlock()
	worker := te.outbox.workerFor(evt.Did)
	worker.inFlightSentAt[evt.ID] = deliveryForEvent(evt)
	err := te.outbox.DeadLetterEvent(te.ctx, evt, payloadTooLargeReason, 413, 1)
	require.NoError(t, err)
	var deadLetter models.OutboxDeadLetter
	require.NoError(t, te.db.Where("original_event_id = ?", evt.ID).First(&deadLetter).Error)

	resp, err := http.Get(te.baseURL() + "/stats/outbox-dead-letter")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/outbox/dead-letters/%d/requeue", te.baseURL(), uint64(math.MaxInt64)+1), nil)
	require.NoError(t, err)
	req.SetBasicAuth("admin", "secret")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	req, err = http.NewRequest(http.MethodGet, te.baseURL()+"/stats/outbox-dead-letter", nil)
	require.NoError(t, err)
	req.SetBasicAuth("admin", "secret")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	var stats map[string]int64
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&stats))
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int64(1), stats["outbox_dead_letter"])

	req, err = http.NewRequest(http.MethodPost, fmt.Sprintf("%s/outbox/dead-letters/%d/requeue", te.baseURL(), deadLetter.ID), nil)
	require.NoError(t, err)
	req.SetBasicAuth("admin", "secret")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var restored models.OutboxBuffer
	require.NoError(t, te.db.First(&restored, "id = ?", active.ID).Error)
	assert.Equal(t, active.Data, restored.Data)
	req, err = http.NewRequest(http.MethodGet, te.baseURL()+"/stats/outbox-dead-letter", nil)
	require.NoError(t, err)
	req.SetBasicAuth("admin", "secret")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&stats))
	resp.Body.Close()
	assert.Zero(t, stats["outbox_dead_letter"])
}

func TestDeadLetterMetricsInitializeAndTrackTransitions(t *testing.T) {
	db := newDeadLetterTestDB(t)
	require.NoError(t, db.Create(&models.OutboxDeadLetter{
		OriginalEventID: 50, Generation: 1, Did: "did:example:metric-existing", Data: `{"id":50}`,
		SHA256: eventSHA256([]byte(`{"id":50}`)), PayloadBytes: int64(len(`{"id":50}`)), Reason: payloadTooLargeReason, DeadLetteredAt: time.Now(),
	}).Error)
	config := &TapConfig{EventCacheSize: 10, OutboxParallelism: 1, DisableAcks: true, RetryTimeout: time.Minute}
	events := newReadyEventManager(db, config)
	outbox := NewOutbox(slog.Default(), events, config)
	assert.Equal(t, float64(1), testutil.ToFloat64(deadLetterDepth))
	assert.Equal(t, float64(1), testutil.ToFloat64(deadLetterRows))
	assert.Equal(t, float64(len(`{"id":50}`)), testutil.ToFloat64(deadLetterRetainedBytes))
	require.NoError(t, events.AddIdentityEvent(t.Context(), &IdentityEvt{
		Did: "did:example:metric-new", Status: models.AccountStatusActive,
	}, func(*gorm.DB) error { return nil }))
	evt, ok := events.GetEvent(1)
	require.True(t, ok)
	worker := outbox.workerFor(evt.Did)
	worker.inFlightSentAt[evt.ID] = deliveryForEvent(evt)
	beforeSuccess := testutil.ToFloat64(deadLetterRequeueSuccesses)
	require.NoError(t, outbox.DeadLetterEvent(t.Context(), evt, payloadTooLargeReason, 413, 1))
	assert.Equal(t, float64(2), testutil.ToFloat64(deadLetterDepth))
	assert.Equal(t, float64(2), testutil.ToFloat64(deadLetterRows))
	retainedBeforeRequeue := testutil.ToFloat64(deadLetterRetainedBytes)
	var deadLetter models.OutboxDeadLetter
	require.NoError(t, db.Where("original_event_id = ?", evt.ID).First(&deadLetter).Error)
	_, err := outbox.RequeueDeadLetter(t.Context(), deadLetter.ID)
	require.NoError(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(deadLetterDepth))
	assert.Equal(t, float64(2), testutil.ToFloat64(deadLetterRows))
	assert.Equal(t, retainedBeforeRequeue-float64(len(evt.Event)), testutil.ToFloat64(deadLetterRetainedBytes))
	assert.Equal(t, beforeSuccess+1, testutil.ToFloat64(deadLetterRequeueSuccesses))
}

func newDeadLetterTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dbURL := "sqlite://" + t.TempDir() + "/tap.db"
	db, err := SetupDatabase(dbURL, 1)
	require.NoError(t, err)
	t.Cleanup(func() {
		sqlDB, sqlErr := db.DB()
		if sqlErr == nil {
			sqlDB.Close()
		}
	})
	return db
}

func newReadyEventManager(db *gorm.DB, config *TapConfig) *EventManager {
	events := NewEventManager(slog.Default(), db, config)
	events.markInitialLoadDone()
	return events
}

func waitForMetricReconciliation(t *testing.T, outbox *Outbox) {
	t.Helper()
	require.Eventually(t, func() bool {
		return !outbox.metricPending.Load()
	}, time.Second, time.Millisecond)
}

func TestWebhookSendStopsDuringRetryBackoffOnContextCancellation(t *testing.T) {
	requestSeen := make(chan struct{}, 1)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requestSeen <- struct{}{}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer receiver.Close()

	client := &WebhookClient{
		logger:     slog.Default(),
		webhookURL: receiver.URL,
		httpClient: &http.Client{Timeout: time.Second},
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var acked, deadLettered atomic.Bool
	go func() {
		client.Send(ctx, &OutboxEvt{ID: 1, Event: []byte(`{"id":1}`), Generation: 1}, func(*OutboxEvt) {
			acked.Store(true)
		}, func(context.Context, *OutboxEvt, string, int, int) error {
			deadLettered.Store(true)
			return nil
		})
		close(done)
	}()

	select {
	case <-requestSeen:
	case <-time.After(time.Second):
		t.Fatal("webhook request did not arrive")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("webhook retry did not stop after context cancellation")
	}
	assert.False(t, acked.Load())
	assert.False(t, deadLettered.Load())
}
