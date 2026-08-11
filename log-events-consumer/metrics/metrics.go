package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	processedMessageCount prometheus.Counter
	processedTickGauge    prometheus.Gauge
	processingEpochGauge  prometheus.Gauge
	consumedTickGauge     prometheus.Gauge
	consumedEpochGauge    prometheus.Gauge
}

func NewMetrics(metricsNamespace string) *Metrics {
	metrics := Metrics{
		processedMessageCount: promauto.NewCounter(prometheus.CounterOpts{
			Name: metricsNamespace + "_processed_message_count",
			Help: "The number of processed message records.",
		}),
		processedTickGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: metricsNamespace + "_processed_tick",
			Help: "The last processed tick.",
		}),
		processingEpochGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: metricsNamespace + "_processed_epoch",
			Help: "The current processing epoch.",
		}),
		consumedTickGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: metricsNamespace + "_consumed_tick",
			Help: "The last tick seen on the topic, including ticks whose logs were all filtered out.",
		}),
		consumedEpochGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: metricsNamespace + "_consumed_epoch",
			Help: "The epoch of the last consumed tick.",
		}),
	}
	return &metrics
}

func (m *Metrics) IncProcessedMessages() {
	m.processedMessageCount.Inc()
}

func (m *Metrics) SetProcessedTick(epoch, tick uint32) {
	m.processingEpochGauge.Set(float64(epoch))
	m.processedTickGauge.Set(float64(tick))
}

func (m *Metrics) SetConsumedTick(epoch, tick uint32) {
	m.consumedEpochGauge.Set(float64(epoch))
	m.consumedTickGauge.Set(float64(tick))
}
