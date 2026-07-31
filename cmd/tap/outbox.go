package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/indigo/cmd/tap/models"
	"github.com/puzpuzpuz/xsync/v4"
)

// Ordering guarantees for events belonging to the same DID:
//
// Live events are synchronization barriers - all prior events must complete
// before a live event can be sent, and the live event must complete before
// any subsequent events can be sent.
//
// Historical events can be sent concurrently with each other (no ordering
// between them), but cannot be sent while a live event is in-flight.
//
// Example sequence: H1, H2, L1, L2, H3, H4, L2, H5
//   - H1 and H2 sent concurrently
//   - Wait for H1 and H2 to complete, then send L1 (alone)
//   - Wait for L1 to complete, then send L2 (alone)
//   - Wait for L2 to complete, then send H3 and H4 concurrently
//   - Wait for H3 and H4 to complete, then send L3 (alone)
//   - Wait for L3 to complete, then send H5

type Outbox struct {
	logger              *slog.Logger
	mode                OutboxMode
	parallelism         int
	retryTimeout        time.Duration
	deleteFlushInterval time.Duration
	deleteRetryBase     time.Duration
	deleteRetryMax      time.Duration
	webhook             *WebhookClient

	events *EventManager

	didWorkers   *xsync.Map[string, *DIDWorker]
	transitionMu sync.Mutex

	acks     chan outboxAck
	outgoing chan *OutboxEvt

	ctx    context.Context
	cancel context.CancelFunc

	deliveryMu sync.Mutex
	stopping   bool
	deliveryWG sync.WaitGroup

	metricTimeout       time.Duration
	metricReconcileHook func(context.Context) error
	metricPending       atomic.Bool
	metricDirty         atomic.Bool

	ackDraining atomic.Bool
	ackMu       sync.RWMutex
	ackDrain    chan struct{}
	ackTimeout  time.Duration
}

func NewOutbox(logger *slog.Logger, events *EventManager, config *TapConfig) *Outbox {
	if err := validateIntSetting("outbox-parallelism", config.OutboxParallelism, maxOutboxParallelism); err != nil {
		panic(err)
	}
	if err := validateIntSetting("outbox-capacity", config.EventCacheSize, maxCacheEntries); err != nil {
		panic(err)
	}
	lifetimeCtx, cancel := context.WithCancel(context.Background())
	o := &Outbox{
		logger:              logger.With("component", "outbox"),
		mode:                parseOutboxMode(config.WebhookURL, config.DisableAcks),
		parallelism:         config.OutboxParallelism,
		retryTimeout:        config.RetryTimeout,
		deleteFlushInterval: 10 * time.Second,
		deleteRetryBase:     time.Second,
		deleteRetryMax:      10 * time.Second,
		webhook: &WebhookClient{
			logger:        logger.With("component", "webhook_client"),
			webhookURL:    config.WebhookURL,
			adminPassword: config.AdminPassword,
			httpClient: &http.Client{
				Timeout: 30 * time.Second,
			},
		},
		events:        events,
		didWorkers:    xsync.NewMap[string, *DIDWorker](),
		acks:          make(chan outboxAck, config.OutboxParallelism*10000),
		outgoing:      make(chan *OutboxEvt, config.OutboxParallelism*10000),
		ctx:           lifetimeCtx,
		cancel:        cancel,
		metricTimeout: 2 * time.Second,
		ackDrain:      make(chan struct{}),
		ackTimeout:    10 * time.Second,
	}
	metricCtx, metricCancel := context.WithTimeout(context.Background(), o.metricTimeout)
	if err := o.refreshDeadLetterMetrics(metricCtx); err != nil {
		o.logger.Error("failed to initialize dead-letter depth metric", "error", err)
	}
	metricCancel()
	events.SetDispatcher(o.dispatchEvent)
	return o
}

// Run starts the outbox workers for event delivery and cleanup.
func (o *Outbox) Run(ctx context.Context) {
	stopLink := context.AfterFunc(ctx, o.cancel)
	defer stopLink()
	if ctx.Err() != nil {
		o.cancel()
	}

	var auxiliaryWorkers sync.WaitGroup
	var deleteWorkers sync.WaitGroup
	deleteCtx, cancelDelete := context.WithCancel(context.Background())

	if o.mode == OutboxModeWebsocketAck {
		auxiliaryWorkers.Add(1)
		go func() {
			defer auxiliaryWorkers.Done()
			o.checkTimeouts(o.ctx)
		}()
	}

	for i := 0; i < o.parallelism; i++ {
		deleteWorkers.Add(1)
		go func() {
			defer deleteWorkers.Done()
			o.runBatchedDeletes(deleteCtx, o.ackDrain)
		}()
	}

	<-o.ctx.Done()
	o.deliveryMu.Lock()
	o.stopping = true
	o.deliveryMu.Unlock()
	auxiliaryWorkers.Wait()
	o.deliveryWG.Wait()
	o.ackMu.Lock()
	o.ackDraining.Store(true)
	close(o.ackDrain)
	o.ackMu.Unlock()
	forceStop := time.AfterFunc(o.ackTimeout, cancelDelete)
	deleteWorkers.Wait()
	if !forceStop.Stop() {
		<-deleteCtx.Done()
	}
	cancelDelete()
}

// workerFor gets or creates the DIDWorker for the given DID.
func (o *Outbox) workerFor(did string) *DIDWorker {
	w, _ := o.didWorkers.LoadOrCompute(did, func() (*DIDWorker, bool) {
		return &DIDWorker{
			did:            did,
			notifChan:      make(chan struct{}, 1),
			inFlightSentAt: make(map[uint]inFlightDelivery),
			seen:           make(map[deliveryIdentity]struct{}),
			outbox:         o,
			ctx:            o.ctx,
		}, false
	})
	return w
}

func (o *Outbox) sendEvent(evt *OutboxEvt) {
	eventsDelivered.Inc()
	o.launchDelivery(func() {
		switch o.mode {
		case OutboxModeFireAndForget, OutboxModeWebsocketAck:
			select {
			case <-o.ctx.Done():
			case o.outgoing <- evt:
			}
		case OutboxModeWebhook:
			o.webhook.Send(o.ctx, evt, o.AckWebhookEvent, o.DeadLetterEvent)
		}
	})
}

func (o *Outbox) launchDelivery(delivery func()) bool {
	o.deliveryMu.Lock()
	if o.stopping || o.ctx.Err() != nil {
		o.deliveryMu.Unlock()
		return false
	}
	o.deliveryWG.Add(1)
	o.deliveryMu.Unlock()
	go func() {
		defer o.deliveryWG.Done()
		delivery()
	}()
	return true
}

// DeadLetterEvent durably moves an active event before releasing its DID barrier.
func (o *Outbox) DeadLetterEvent(ctx context.Context, evt *OutboxEvt, reason string, httpStatus, attempts int) error {
	o.transitionMu.Lock()
	result, err := o.events.DeadLetterEvent(ctx, evt, reason, httpStatus, attempts)
	if err != nil {
		deadLetterPersistenceFailures.WithLabelValues(deadLetterErrorCategory(err)).Inc()
		o.transitionMu.Unlock()
		return err
	}

	if result.created {
		eventsDeadLettered.Inc()
		deadLetterDepth.Inc()
		deadLetterRows.Inc()
		deadLetterRetainedBytes.Add(float64(result.deadLetter.PayloadBytes))
	}
	if worker, ok := o.didWorkers.Load(result.deadLetter.Did); ok {
		worker.ackEvent(evt)
		worker.forgetEvent(evt)
	}
	o.transitionMu.Unlock()
	if !result.created {
		o.scheduleDeadLetterMetricsRefresh()
	}
	return nil
}

// AckEvent marks an event as delivered and queues it for deletion.
func (o *Outbox) AckEvent(eventID uint) {
	evt, exists := o.events.GetEvent(eventID)
	if !exists {
		return
	}
	o.ackEvent(evt)
}

func (o *Outbox) AckWebhookEvent(evt *OutboxEvt) {
	o.ackEvent(evt)
}

func (o *Outbox) ackEvent(evt *OutboxEvt) {
	o.transitionMu.Lock()
	current, exists := o.events.GetEvent(evt.ID)
	if !exists || !outboxEventsMatch(current, evt) {
		o.transitionMu.Unlock()
		return
	}
	if worker, ok := o.didWorkers.Load(evt.Did); ok {
		worker.ackEvent(evt)
	}
	eventsAcked.Inc()
	ack := outboxAck{ID: evt.ID, Generation: evt.Generation, Did: evt.Did, SHA256: eventSHA256(evt.Event)}
	o.transitionMu.Unlock()
	o.submitAck(ack)
}

func (o *Outbox) submitAck(ack outboxAck) {
	o.ackMu.RLock()
	if o.ackDraining.Load() {
		o.ackMu.RUnlock()
		o.persistAck(ack)
		return
	}
	select {
	case o.acks <- ack:
		o.ackMu.RUnlock()
		return
	default:
		o.ackMu.RUnlock()
		o.persistAck(ack)
	}
}

func (o *Outbox) persistAck(ack outboxAck) {
	ctx, cancel := context.WithTimeout(context.Background(), o.ackTimeout)
	defer cancel()
	for retries := 0; ; retries++ {
		if err := o.events.DeleteEvents(ctx, []outboxAck{ack}); err == nil {
			if worker, ok := o.didWorkers.Load(ack.Did); ok {
				worker.forget(ack)
			}
			return
		} else if ctx.Err() != nil {
			o.logger.Error("failed to persist acknowledged event before timeout", "id", ack.ID, "generation", ack.Generation, "error", err)
			return
		}
		if !sleepContext(ctx, boundedDeleteBackoff(retries, o.deleteRetryBase, o.deleteRetryMax)) {
			return
		}
	}
}

func (o *Outbox) RequeueDeadLetter(ctx context.Context, deadLetterID uint) (*OutboxEvt, error) {
	if err := o.events.WaitForInitialLoad(ctx); err != nil {
		deadLetterRequeueFailures.WithLabelValues(deadLetterErrorCategory(err)).Inc()
		return nil, err
	}

	o.transitionMu.Lock()
	result, err := o.events.RequeueDeadLetter(ctx, deadLetterID)
	if err != nil {
		deadLetterRequeueFailures.WithLabelValues(deadLetterErrorCategory(err)).Inc()
		o.transitionMu.Unlock()
		return nil, err
	}
	if result.compactedBytes > 0 {
		deadLetterDepth.Dec()
		deadLetterRetainedBytes.Sub(float64(result.compactedBytes))
	}
	deadLetterRequeueSuccesses.Inc()
	var action dispatchAction
	if result.shouldSend {
		action = o.enqueueEventLocked(result.event)
	}
	o.transitionMu.Unlock()
	action.activate(o)
	if result.compactedBytes == 0 {
		o.scheduleDeadLetterMetricsRefresh()
	}
	return result.event, nil
}

func (o *Outbox) refreshDeadLetterMetrics(ctx context.Context) error {
	var depth, rows, retainedBytes int64
	db := o.events.db.WithContext(ctx)
	if err := db.Model(&models.OutboxDeadLetter{}).Where("requeued_at IS NULL").Count(&depth).Error; err != nil {
		return err
	}
	if err := db.Model(&models.OutboxDeadLetter{}).Count(&rows).Error; err != nil {
		return err
	}
	if err := db.Model(&models.OutboxDeadLetter{}).Select("COALESCE(SUM(payload_bytes), 0)").Where("data <> ''").Scan(&retainedBytes).Error; err != nil {
		return err
	}
	deadLetterDepth.Set(float64(depth))
	deadLetterRows.Set(float64(rows))
	deadLetterRetainedBytes.Set(float64(retainedBytes))
	return nil
}

func (o *Outbox) scheduleDeadLetterMetricsRefresh() {
	o.metricDirty.Store(true)
	if !o.metricPending.CompareAndSwap(false, true) {
		return
	}
	if !o.launchDelivery(func() {
		defer func() {
			o.metricPending.Store(false)
			if o.metricDirty.Load() {
				o.scheduleDeadLetterMetricsRefresh()
			}
		}()
		for {
			o.metricDirty.Store(false)
			ctx, cancel := context.WithTimeout(o.ctx, o.metricTimeout)
			var err error
			if o.metricReconcileHook != nil {
				err = o.metricReconcileHook(ctx)
			}
			if err == nil {
				err = o.refreshDeadLetterMetrics(ctx)
			}
			cancel()
			if err != nil && !errors.Is(err, context.Canceled) {
				o.logger.Error("failed to reconcile dead-letter metrics", "error", err)
			}
			if !o.metricDirty.Load() {
				return
			}
		}
	}) {
		o.metricPending.Store(false)
	}
}

func (o *Outbox) dispatchEvent(expected *OutboxEvt) {
	select {
	case <-o.ctx.Done():
		return
	default:
	}

	o.transitionMu.Lock()
	current, exists := o.events.GetEvent(expected.ID)
	if !exists || !outboxEventsMatch(current, expected) {
		o.transitionMu.Unlock()
		return
	}
	action := o.enqueueEventLocked(current)
	o.transitionMu.Unlock()
	action.activate(o)
}

func (o *Outbox) enqueueEventLocked(evt *OutboxEvt) dispatchAction {
	return o.workerFor(evt.Did).enqueueEvent(evt)
}

func deadLetterErrorCategory(err error) string {
	switch {
	case errors.Is(err, errDeadLetterNotFound):
		return "not_found"
	case errors.Is(err, errDeadLetterConflict), errors.Is(err, errActiveEventConflict):
		return "conflict"
	case errors.Is(err, errDeadLetterTooLarge):
		return "undeliverable"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled"
	default:
		return "database"
	}
}

func (o *Outbox) runBatchedDeletes(ctx context.Context, drain <-chan struct{}) {
	ticker := time.NewTicker(o.deleteFlushInterval)
	defer ticker.Stop()

	var batch []outboxAck
	seen := make(map[outboxAck]struct{})
	var retryTimer *time.Timer
	var retryC <-chan time.Time
	retries := 0
	draining := false

	stopRetry := func() {
		if retryTimer != nil && !retryTimer.Stop() {
			select {
			case <-retryTimer.C:
			default:
			}
		}
		retryC = nil
	}
	defer stopRetry()

	flush := func() {
		if len(batch) == 0 || retryC != nil {
			return
		}
		if err := o.events.DeleteEvents(ctx, batch); err != nil {
			o.logger.Error("failed to delete batch of acked events", "error", err, "count", len(batch))
			delay := boundedDeleteBackoff(retries, o.deleteRetryBase, o.deleteRetryMax)
			retries++
			if retryTimer == nil {
				retryTimer = time.NewTimer(delay)
			} else {
				retryTimer.Reset(delay)
			}
			retryC = retryTimer.C
			return
		}
		for _, ack := range batch {
			if worker, ok := o.didWorkers.Load(ack.Did); ok {
				worker.forget(ack)
			}
		}
		batch = nil
		clear(seen)
		retries = 0
	}

	for {
		if draining && retryC == nil {
			for {
				select {
				case ack := <-o.acks:
					if _, exists := seen[ack]; !exists {
						seen[ack] = struct{}{}
						batch = append(batch, ack)
					}
				default:
					flush()
					if len(batch) == 0 {
						return
					}
					goto waitForRetry
				}
			}
		}
	waitForRetry:
		select {
		case <-ctx.Done():
			return
		case <-drain:
			draining = true
			drain = nil
		case ack := <-o.acks:
			if _, exists := seen[ack]; !exists {
				seen[ack] = struct{}{}
				batch = append(batch, ack)
			}
			if len(batch) >= 1000 {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-retryC:
			retryC = nil
			flush()
		}
	}
}

func boundedDeleteBackoff(retries int, base, max time.Duration) time.Duration {
	delay := base
	for i := 0; i < retries && delay < max; i++ {
		if delay > max/2 {
			return max
		}
		delay *= 2
	}
	if delay > max {
		return max
	}
	return delay
}

func (o *Outbox) checkTimeouts(ctx context.Context) {
	runPeriodically(ctx, o.retryTimeout, func(ctx context.Context) error {
		o.retryTimedOutEvents()
		return nil
	})
}

// retryTimedOutEvents iterates through all workers and re-queues timed out events
func (o *Outbox) retryTimedOutEvents() {
	// Get snapshot of all active workers
	workers := make([]*DIDWorker, 0)
	o.didWorkers.Range(func(key string, value *DIDWorker) bool {
		workers = append(workers, value)
		return true
	})

	for _, worker := range workers {
		timedOutIDs := worker.timedOutEvents()
		for _, id := range timedOutIDs {
			evt, exists := o.events.GetEvent(id)
			if exists {
				o.logger.Info("retrying timed out event", "id", id)
				o.sendEvent(evt)
			}
		}
	}
}

type DIDWorker struct {
	outbox         *Outbox
	ctx            context.Context
	did            string
	notifChan      chan struct{}
	pendingEvts    []*OutboxEvt
	inFlightSentAt map[uint]inFlightDelivery
	seen           map[deliveryIdentity]struct{}
	blockedOnLive  bool
	initialPending bool
	running        bool
	mu             sync.Mutex
}

type dispatchAction struct {
	worker    *DIDWorker
	immediate *OutboxEvt
	start     bool
	notify    bool
}

func (a dispatchAction) activate(outbox *Outbox) {
	if a.worker == nil {
		return
	}
	if a.immediate != nil {
		outbox.sendEvent(a.immediate)
		if a.worker.completeInitialSend() {
			outbox.launchDelivery(a.worker.run)
		}
		a.worker.notify()
		return
	}
	if a.start {
		outbox.launchDelivery(a.worker.run)
	}
	if a.notify {
		a.worker.notify()
	}
}

type inFlightDelivery struct {
	sentAt     time.Time
	generation uint64
	sha256     string
}

type deliveryIdentity struct {
	id         uint
	generation uint64
	sha256     string
}

func identityForEvent(evt *OutboxEvt) deliveryIdentity {
	return deliveryIdentity{id: evt.ID, generation: evt.Generation, sha256: eventSHA256(evt.Event)}
}

func deliveryForEvent(evt *OutboxEvt) inFlightDelivery {
	return inFlightDelivery{
		sentAt:     time.Now(),
		generation: evt.Generation,
		sha256:     eventSHA256(evt.Event),
	}
}

func (d inFlightDelivery) matches(evt *OutboxEvt) bool {
	return d.generation == evt.Generation && d.sha256 == eventSHA256(evt.Event)
}

func (w *DIDWorker) run() {
	for {
		// Queue up all possible pending events
		w.processPendingEvts()

		w.mu.Lock()
		queueEmpty := len(w.pendingEvts) == 0
		noInFlight := len(w.inFlightSentAt) == 0
		if noInFlight {
			w.blockedOnLive = false
		}
		hasReadyWork := !queueEmpty && !w.blockedOnLive
		allDone := queueEmpty && noInFlight
		if allDone {
			w.running = false
		}
		w.mu.Unlock()

		// All work is done — stop the worker goroutine
		if allDone {
			return
		}

		// If there are still pending events, loop immediately without waiting for a notification
		if hasReadyWork {
			continue
		}

		// Wait for an ack or new event before retrying
		select {
		case <-w.ctx.Done():
			return
		case <-w.notifChan:
		}
	}
}

// get as many pending events in flight as we can
// returns when it hits a blocking event
func (w *DIDWorker) processPendingEvts() {
	for {
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		w.mu.Lock()
		if w.blockedOnLive {
			w.mu.Unlock()
			return
		}

		if len(w.pendingEvts) == 0 {
			w.mu.Unlock()
			return
		}
		evt := w.pendingEvts[0]
		w.mu.Unlock()

		current, exists := w.outbox.events.GetEvent(evt.ID)
		if !exists || !outboxEventsMatch(current, evt) {
			// Event was already acked/removed, skip it
			w.mu.Lock()
			w.pendingEvts = w.pendingEvts[1:]
			w.mu.Unlock()
			continue
		}

		w.mu.Lock()
		if _, sameIDInFlight := w.inFlightSentAt[evt.ID]; sameIDInFlight {
			w.mu.Unlock()
			return
		}
		if evt.Live {
			w.blockedOnLive = true

			hasInFlight := len(w.inFlightSentAt) > 0
			// live event - must wait for all in-flight to clear
			if hasInFlight {
				w.mu.Unlock()
				return
			}
		}
		w.pendingEvts = w.pendingEvts[1:]
		w.inFlightSentAt[evt.ID] = deliveryForEvent(evt)
		w.mu.Unlock()

		w.outbox.sendEvent(evt)
		if evt.Live {
			return // not going to be able to send anymore in this loop so return for now
		}
	}
}

func (w *DIDWorker) enqueueEvent(evt *OutboxEvt) dispatchAction {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.seen == nil {
		w.seen = make(map[deliveryIdentity]struct{})
	}
	identity := identityForEvent(evt)
	if _, exists := w.seen[identity]; exists {
		return dispatchAction{}
	}
	if delivery, exists := w.inFlightSentAt[evt.ID]; exists && delivery.matches(evt) {
		return dispatchAction{}
	}
	for _, pending := range w.pendingEvts {
		if outboxEventsMatch(pending, evt) {
			return dispatchAction{}
		}
	}
	w.seen[identity] = struct{}{}

	// Fast path: no contention, send immediately without goroutine
	if len(w.inFlightSentAt) == 0 && !w.blockedOnLive && !w.initialPending && len(w.pendingEvts) == 0 {
		w.inFlightSentAt[evt.ID] = deliveryForEvent(evt)
		w.initialPending = true
		if evt.Live {
			w.blockedOnLive = true
		}
		return dispatchAction{worker: w, immediate: evt}
	}

	// Slow path: contention exists, need goroutine for ordering
	w.pendingEvts = append(w.pendingEvts, evt)
	start := false
	if !w.running && !w.initialPending {
		w.running = true
		start = true
	}
	return dispatchAction{worker: w, start: start, notify: true}
}

func (w *DIDWorker) forget(ack outboxAck) {
	w.mu.Lock()
	delete(w.seen, deliveryIdentity{id: ack.ID, generation: ack.Generation, sha256: ack.SHA256})
	w.mu.Unlock()
}

func (w *DIDWorker) forgetEvent(evt *OutboxEvt) {
	w.mu.Lock()
	delete(w.seen, identityForEvent(evt))
	w.mu.Unlock()
}

func (w *DIDWorker) completeInitialSend() bool {
	w.mu.Lock()
	w.initialPending = false
	start := len(w.pendingEvts) > 0 && !w.running
	if start {
		w.running = true
	}
	w.mu.Unlock()
	return start
}

func (w *DIDWorker) notify() {
	select {
	case w.notifChan <- struct{}{}:
	default:
	}
}

func (w *DIDWorker) ackEvent(evt *OutboxEvt) bool {
	w.mu.Lock()
	delivery, exists := w.inFlightSentAt[evt.ID]
	matched := exists && delivery.matches(evt)
	if matched {
		delete(w.inFlightSentAt, evt.ID)
	}
	w.mu.Unlock()
	if !matched {
		return false
	}

	select {
	case w.notifChan <- struct{}{}:
	default:
	}
	return true
}

// checkAndRetryTimeouts checks for timed out events and returns their IDs
// Must be called without holding w.mu
func (w *DIDWorker) timedOutEvents() []uint {
	w.mu.Lock()
	defer w.mu.Unlock()

	var timedOut []uint
	now := time.Now()

	for evtId, delivery := range w.inFlightSentAt {
		if now.Sub(delivery.sentAt) > w.outbox.retryTimeout {
			timedOut = append(timedOut, evtId)
		}
	}

	return timedOut
}
