package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Firehose metrics
var (
	firehoseEventsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tap_firehose_events_received_total",
		Help: "Total number of events received from firehose",
	})
	firehoseEventsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tap_firehose_events_processed_total",
		Help: "Total number of events successfully processed",
	})
	firehoseEventsSkipped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tap_firehose_events_skipped_total",
		Help: "Total number of events skipped (not tracked)",
	})
	firehoseLastSeq = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tap_firehose_last_seq",
		Help: "Sequence number of last processed firehose event",
	})
)

// Buffer metrics
var (
	eventCacheSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tap_event_cache_size",
		Help: "Number of events in memory cache",
	})
)

// Resync metrics
var (
	resyncsStarted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tap_resyncs_started_total",
		Help: "Total number of repo resyncs started",
	})
	resyncsCompleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tap_resyncs_completed_total",
		Help: "Total number of repo resyncs completed",
	})
	resyncsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tap_resyncs_failed_total",
		Help: "Total number of repo resyncs failed",
	})
	resyncDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "tap_resync_duration_seconds",
		Help:    "Duration of repo resync operations",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 12),
	})
)

// Outbox delivery metrics
var (
	eventsDelivered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tap_events_delivered_total",
		Help: "Total number of events delivered to clients",
	})
	eventsAcked = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tap_events_acked_total",
		Help: "Total number of events acknowledged",
	})
	eventsDeadLettered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tap_events_dead_lettered_total",
		Help: "Total number of events durably moved to the webhook dead-letter queue",
	})
	deadLetterDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tap_outbox_dead_letter_depth",
		Help: "Number of webhook events currently awaiting dead-letter requeue",
	})
	deadLetterRows = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tap_outbox_dead_letter_rows",
		Help: "Total durable dead-letter rows, including compacted requeue receipts",
	})
	deadLetterRetainedBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tap_outbox_dead_letter_retained_body_bytes",
		Help: "Bytes of JSON bodies still retained in active dead-letter rows",
	})
	deadLetterRequeueSuccesses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tap_outbox_dead_letter_requeue_success_total",
		Help: "Total successful dead-letter requeue requests",
	})
	deadLetterRequeueFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tap_outbox_dead_letter_requeue_failure_total",
		Help: "Total failed dead-letter requeue requests by bounded category",
	}, []string{"category"})
	deadLetterPersistenceFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tap_outbox_dead_letter_persistence_failure_total",
		Help: "Total failed dead-letter persistence attempts by bounded category",
	}, []string{"category"})
	webhookRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tap_webhook_requests_total",
		Help: "Total webhook requests by status",
	}, []string{"status"})
)

// Crawler metrics
var (
	crawlerReposDiscovered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tap_crawler_repos_discovered_total",
		Help: "Total repos discovered by crawler",
	})
)
