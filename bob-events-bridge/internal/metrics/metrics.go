package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/qubic/bob-events-bridge/internal/bob"
)

type BridgeMetrics struct {
	kafkaMessagesPublishedTotalCounter *prometheus.CounterVec
	kafkaPublishErrorsTotalCounter     *prometheus.CounterVec

	processorEventsReceivedTotalCounter     *prometheus.CounterVec
	processorEventsProcessedTotalCounter    *prometheus.CounterVec
	processorEventsDeduplicatedTotalCounter *prometheus.CounterVec
	processorEventsFailedTotalCounter       *prometheus.CounterVec
	processorEventsSkippedNonOKTotalCounter prometheus.Counter
	processorReconnectionsTotalCounter      prometheus.Counter
	processorBobErrorsTotalCounter          *prometheus.CounterVec

	stateCurrentEpochGauge          prometheus.Gauge
	stateLastProcessedTickGauge     prometheus.Gauge
	stateLastProcessedLogIDGauge    prometheus.Gauge
	stateProcessingDeltaGauge       prometheus.Gauge
	stateCurrentTickEventCountGauge prometheus.Gauge

	healthProcessorRunningGauge prometheus.Gauge
}

func NewBridgeMetrics(registerer prometheus.Registerer, namespace string) *BridgeMetrics {

	factory := promauto.With(registerer)

	metrics := BridgeMetrics{
		// Kafka metrics
		kafkaMessagesPublishedTotalCounter: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "kafka",
			Name:      "messages_published_total",
			Help:      "Total number of messages published to Kafka",
		},
			[]string{"event_type", "event_type_name", "topic"},
		),
		kafkaPublishErrorsTotalCounter: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: "kafka",
				Name:      "publish_errors_total",
				Help:      "Total number of Kafka publish errors",
			},
			[]string{"error_type"},
		),

		// Processor metrics
		//
		// NOTE: These counters track per-attempt counts, not unique events. Due to
		// at-least-once delivery, when a batch flush fails (kafka/storage error) the
		// events are marked as failed, the processor reconnects, and bob resends them.
		// On retry the same events will increment events_received again and, if
		// successful, events_processed. This means a single logical event can appear
		// in both events_failed and events_processed, and events_received can count
		// it more than once. The invariant
		//   received = processed + deduplicated + failed + pending
		// holds within a single connection session but not across retries.
		processorEventsReceivedTotalCounter: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "processor",
			Name:      "events_received_total",
			Help:      "Total number of events received from bob",
		},
			[]string{"event_type", "event_type_name"},
		),
		processorEventsProcessedTotalCounter: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "processor",
			Name:      "events_processed_total",
			Help:      "Total number of events successfully processed",
		},
			[]string{"event_type", "event_type_name"},
		),
		processorEventsDeduplicatedTotalCounter: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "processor",
			Name:      "events_deduplicated_total",
			Help:      "Total number of duplicate events skipped",
		},
			[]string{"event_type", "event_type_name"},
		),
		processorEventsFailedTotalCounter: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "processor",
			Name:      "events_failed_total",
			Help:      "Total number of events that failed processing",
		},
			[]string{"event_type", "event_type_name", "failure_reason"},
		),
		processorEventsSkippedNonOKTotalCounter: factory.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "processor",
			Name:      "events_skipped_non_ok_total",
			Help:      "Total number of events skipped due to non-OK status from bob",
		}),
		processorReconnectionsTotalCounter: factory.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "processor",
			Name:      "reconnections_total",
			Help:      "Total number of WebSocket reconnections to bob",
		}),
		processorBobErrorsTotalCounter: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "processor",
			Name:      "bob_errors_total",
			Help:      "Total number of error messages received from bob",
		},
			[]string{"error_code"},
		),

		// State metrics
		stateCurrentEpochGauge: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "state",
			Name:      "current_epoch",
			Help:      "Current epoch being processed",
		}),
		stateLastProcessedTickGauge: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "state",
			Name:      "last_processed_tick",
			Help:      "Last tick number processed",
		}),
		stateLastProcessedLogIDGauge: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "state",
			Name:      "last_processed_log_id",
			Help:      "Last log ID processed",
		}),
		stateProcessingDeltaGauge: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "state",
			Name:      "processing_delta",
			Help:      "Difference between current tick and last processed tick",
		}),
		stateCurrentTickEventCountGauge: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "state",
			Name:      "current_tick_event_count",
			Help:      "Number of events in the current tick being processed",
		}),

		// Health metrics
		healthProcessorRunningGauge: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "health",
			Name:      "processor_running",
			Help:      "Whether the processor is currently running (1 = running, 0 = stopped)",
		}),
	}

	return &metrics
}

// Kafka metrics methods

func (m *BridgeMetrics) IncKafkaMessagesPublished(eventType uint32, topic string) {
	m.kafkaMessagesPublishedTotalCounter.WithLabelValues(
		strconv.FormatUint(uint64(eventType), 10),
		bob.GetLogTypeName(eventType),
		topic,
	).Inc()
}

func (m *BridgeMetrics) IncKafkaPublishErrors(errorType string) {
	m.kafkaPublishErrorsTotalCounter.WithLabelValues(errorType).Inc()
}

// Processor metrics methods

func (m *BridgeMetrics) IncProcessorEventsReceived(eventType uint32) {
	m.processorEventsReceivedTotalCounter.WithLabelValues(
		strconv.FormatUint(uint64(eventType), 10),
		bob.GetLogTypeName(eventType),
	).Inc()
}

func (m *BridgeMetrics) IncProcessorEventsProcessed(eventType uint32) {
	m.processorEventsProcessedTotalCounter.WithLabelValues(
		strconv.FormatUint(uint64(eventType), 10),
		bob.GetLogTypeName(eventType),
	).Inc()
}

func (m *BridgeMetrics) IncProcessorEventsDeduplicated(eventType uint32) {
	m.processorEventsDeduplicatedTotalCounter.WithLabelValues(
		strconv.FormatUint(uint64(eventType), 10),
		bob.GetLogTypeName(eventType),
	).Inc()
}

func (m *BridgeMetrics) IncProcessorEventsSkippedNonOK() {
	m.processorEventsSkippedNonOKTotalCounter.Inc()
}

func (m *BridgeMetrics) IncProcessorEventsFailed(eventType uint32, failureReason string) {
	m.processorEventsFailedTotalCounter.WithLabelValues(
		strconv.FormatUint(uint64(eventType), 10),
		bob.GetLogTypeName(eventType),
		failureReason,
	).Inc()
}

func (m *BridgeMetrics) IncProcessorReconnections() {
	m.processorReconnectionsTotalCounter.Inc()
}

func (m *BridgeMetrics) IncProcessorBobErrors(errorCode string) {
	m.processorBobErrorsTotalCounter.WithLabelValues(errorCode).Inc()
}

// State metrics methods

func (m *BridgeMetrics) SetCurrentEpoch(epoch uint16) {
	m.stateCurrentEpochGauge.Set(float64(epoch))
}

func (m *BridgeMetrics) SetLastProcessedTick(tick uint32) {
	m.stateLastProcessedTickGauge.Set(float64(tick))
}

func (m *BridgeMetrics) SetLastProcessedLogID(logID uint64) {
	m.stateLastProcessedLogIDGauge.Set(float64(logID))
}

func (m *BridgeMetrics) SetProcessingDelta(delta int64) {
	m.stateProcessingDeltaGauge.Set(float64(delta))
}

func (m *BridgeMetrics) SetCurrentTickEventCount(count int) {
	m.stateCurrentTickEventCountGauge.Set(float64(count))
}

// Health metrics methods

func (m *BridgeMetrics) SetProcessorRunning(running bool) {
	if running {
		m.healthProcessorRunningGauge.Set(1)
	} else {
		m.healthProcessorRunningGauge.Set(0)
	}
}
