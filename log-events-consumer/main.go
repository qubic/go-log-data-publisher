package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ardanlabs/conf/v3"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/log-events-consumer/consume"
	"github.com/qubic/log-events-consumer/elastic"
	"github.com/qubic/log-events-consumer/metrics"
	"github.com/qubic/log-events-consumer/status"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
)

func main() {
	err := run()
	if err != nil {
		log.Printf("main: exited with error: %s", err)
		os.Exit(1)
	}
	log.Printf("main: exited successfully.")
}

const configPrefix = "QUBIC_LOG_EVENTS_CONSUMER"

func run() error {
	var cfg struct {
		Elastic struct {
			Addresses   []string `conf:"default:https://localhost:9200"`
			Username    string   `conf:"default:qubic-ingestion"`
			Password    string   `conf:"optional,mask"`
			IndexName   string   `conf:"default:qubic-log-events-alias"`
			Certificate string   `conf:"default:http_ca.crt"`
			MaxRetries  int      `conf:"default:15"`
		}
		Broker struct {
			BootstrapServers []string `conf:"default:localhost:9092"`
			ConsumeTopic     string   `conf:"default:qubic-log-events-data"`
			ConsumerGroup    string   `conf:"default:qubic-elastic"`
		}
		Metrics struct {
			Port      int    `conf:"default:9999"`
			Namespace string `conf:"default:qubic_kafka"`
		}
		Base struct {
			// map with emitting contract index as key and supported log types as values
			SupportedLogTypes map[uint64][]int16 `conf:"default:0:0;1;2;3;8;13"`
		}
	}

	help, err := conf.Parse(configPrefix, &cfg)
	if err != nil {
		if errors.Is(err, conf.ErrHelpWanted) {
			fmt.Println(help)
			return nil
		}

		return fmt.Errorf("parsing config: %w", err)
	}

	out, err := conf.String(&cfg)
	if err != nil {
		return fmt.Errorf("generating config for output: %w", err)
	}
	log.Printf("main: Config :\n%v", out)

	m := kprom.NewMetrics(cfg.Metrics.Namespace,
		kprom.Registerer(prometheus.DefaultRegisterer),
		kprom.Gatherer(prometheus.DefaultGatherer))

	kcl, err := kgo.NewClient(
		kgo.WithHooks(m),
		kgo.SeedBrokers(cfg.Broker.BootstrapServers...),
		kgo.ConsumeTopics(cfg.Broker.ConsumeTopic),
		kgo.ConsumerGroup(cfg.Broker.ConsumerGroup),
		kgo.BlockRebalanceOnPoll(),
		kgo.DisableAutoCommit(),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelInfo, nil)),
	)
	if err != nil {
		return fmt.Errorf("creating kgo client: %w", err)
	}
	defer kcl.Close()

	cert, err := os.ReadFile(cfg.Elastic.Certificate)
	if err != nil {
		log.Printf("[WARN] main: could not read elastic certificate: %v", err)
	}

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:     cfg.Elastic.Addresses,
		Username:      cfg.Elastic.Username,
		Password:      cfg.Elastic.Password,
		CACert:        cert,
		RetryOnStatus: []int{502, 503, 504, 429},
		MaxRetries:    cfg.Elastic.MaxRetries,
		RetryBackoff:  calculateBackoff(),
	})
	if err != nil {
		return fmt.Errorf("creating elasticsearch client: %w", err)
	}
	elasticClient := elastic.NewClient(esClient, cfg.Elastic.IndexName)

	consumeMetrics := metrics.NewMetrics(cfg.Metrics.Namespace)

	consumer := consume.NewConsumer(kcl, elasticClient, consumeMetrics, cfg.Base.SupportedLogTypes)
	procError := make(chan error, 1)

	consumerCtx, consumerCtxCancel := context.WithCancel(context.Background())
	defer consumerCtxCancel()

	go func() {
		procError <- consumer.Consume(consumerCtx)
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	http.HandleFunc("/health", status.Health)
	http.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		Addr: fmt.Sprintf(":%d", cfg.Metrics.Port),
	}

	serverError := make(chan error, 1)
	go func() {
		log.Printf("main: Starting health and metrics endpoint on port [%d].", cfg.Metrics.Port)
		serverError <- srv.ListenAndServe()
	}()

	log.Println("main: Service started.")

	for {
		select {
		case <-shutdown:
			log.Println("main: Received shutdown signal, shutting down...")
			consumerCtxCancel()
			<-procError // Wait for consumer to stop
			log.Println("main: Consumer stopped gracefully")
			shutdownHTTPServer(srv)
			return nil

		case err := <-procError:
			shutdownHTTPServer(srv)
			if err != nil {
				return fmt.Errorf("processing error: %w", err)
			}
			return nil

		case err := <-serverError:
			consumerCtxCancel() // Cancel context to stop consumer
			<-procError         // Wait for consumer to stop
			return fmt.Errorf("server stopped: %w", err)
		}
	}

}

func shutdownHTTPServer(srv *http.Server) {
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("main: HTTP server shutdown error: %v", err)
	} else {
		log.Println("main: HTTP server stopped gracefully")
	}
}

// calculateBackoff needs retry number because of multi threading
func calculateBackoff() func(i int) time.Duration {
	return func(i int) time.Duration {
		var d time.Duration
		if i < 10 {
			d = time.Second*time.Duration(i) + randomMillis()
		} else {
			d = time.Second*30 + randomMillis()
		}
		log.Printf("[WARN] elasticsearch client retry [%d] in %v.", i, d)
		return d
	}
}

func randomMillis() time.Duration {
	return time.Duration(rand.Intn(1000)) * time.Millisecond
}
