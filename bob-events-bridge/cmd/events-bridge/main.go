package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	grpcProm "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/bob-events-bridge/internal/config"
	bridgeGrpc "github.com/qubic/bob-events-bridge/internal/grpc"
	"github.com/qubic/bob-events-bridge/internal/kafka"
	"github.com/qubic/bob-events-bridge/internal/metrics"
	"github.com/qubic/bob-events-bridge/internal/processor"
	"github.com/qubic/bob-events-bridge/internal/storage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	googleGrpc "google.golang.org/grpc"
)

func main() {
	// Parse configuration from env vars and CLI flags
	cfg, err := config.Parse()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse configuration: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	logConfig := zap.NewProductionConfig()
	if cfg.Debug {
		logConfig.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	}
	logger, err := logConfig.Build()
	if err != nil {
		panic("failed to create logger: " + err.Error())
	}
	defer logger.Sync() //nolint:errcheck

	logger.Info("Starting bob-events-bridge",
		zap.String("version", config.Version),
		zap.String("build", config.Build))

	logger.Info("Configuration loaded",
		zap.String("bobWebSocketURL", cfg.Bob.WebSocketURL),
		zap.String("storagePath", cfg.Storage.BasePath),
		zap.String("grpcAddr", cfg.Server.GRPCAddr),
		zap.String("httpAddr", cfg.Server.HTTPAddr))

	if cfg.Bob.OverrideStartTick {
		logger.Info("Start tick override enabled",
			zap.Uint32("startTick", cfg.Bob.StartTick))
	}

	promReg := prometheus.DefaultRegisterer

	grpcMetrics := grpcProm.NewServerMetrics(
		grpcProm.WithServerCounterOptions(grpcProm.WithConstLabels(prometheus.Labels{"namespace": cfg.Metrics.Namespace})),
	)
	promReg.MustRegister(grpcMetrics)

	bridgeMetrics := metrics.NewBridgeMetrics(promReg, cfg.Metrics.Namespace)

	// Parse subscriptions
	subscriptions, err := cfg.GetSubscriptions()
	if err != nil {
		logger.Fatal("Failed to parse subscriptions", zap.Error(err))
	}
	logger.Info("Subscriptions configured", zap.Int("count", len(subscriptions)))

	// Initialize storage manager
	storageMgr, err := storage.NewManager(cfg.Storage.BasePath, logger)
	if err != nil {
		logger.Fatal("Failed to initialize storage", zap.Error(err))
	}
	defer storageMgr.Close() //nolint:errcheck

	logger.Info("Storage initialized",
		zap.Strings("epochs", epochsToStrings(storageMgr.GetAvailableEpochs())))

	// Create and start gRPC server
	server := bridgeGrpc.NewServer(storageMgr, logger,
		googleGrpc.UnaryInterceptor(grpcMetrics.UnaryServerInterceptor()),
		googleGrpc.StreamInterceptor(grpcMetrics.StreamServerInterceptor()),
	)
	if err := server.Start(bridgeGrpc.ServerConfig{
		GRPCAddr: cfg.Server.GRPCAddr,
		HTTPAddr: cfg.Server.HTTPAddr,
	}); err != nil {
		logger.Fatal("Failed to start server", zap.Error(err))
	}

	logger.Info("gRPC and HTTP servers started")

	// Initialize Kafka publisher if enabled
	var kafkaPublisher kafka.Publisher
	if cfg.Kafka.Enabled {
		brokers := strings.Split(cfg.Kafka.Brokers, ",")
		kafkaPublisher = kafka.NewProducer(brokers, cfg.Kafka.Topic, logger, bridgeMetrics)
		defer kafkaPublisher.Close() //nolint:errcheck
		logger.Info("Kafka publisher enabled",
			zap.Strings("brokers", brokers),
			zap.String("topic", cfg.Kafka.Topic))
	}

	// Create processor
	proc := processor.NewProcessor(cfg, subscriptions, storageMgr, logger, kafkaPublisher, bridgeMetrics)

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start processor in goroutine
	processorDone := make(chan error, 1)
	go func() {
		processorDone <- proc.Start(ctx)
	}()

	// Start metrics server
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`{"status":"UP"}`)); err != nil {
			logger.Error("Failed to write health response", zap.Error(err))
		}
	})

	metricsServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Metrics.Port),
		Handler: mux,
	}

	metricsErr := make(chan error, 1)
	go func() {
		logger.Info("Metrics server started", zap.Int("port", cfg.Metrics.Port))
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			metricsErr <- err
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
	case err := <-processorDone:
		if err != nil && err != context.Canceled {
			logger.Error("Processor stopped with error", zap.Error(err))
		}
	case err := <-metricsErr:
		logger.Error("Metrics server stopped with error", zap.Error(err))
	}

	// Graceful shutdown
	logger.Info("Shutting down...")

	// Cancel context to stop processor
	cancel()

	// Stop processor
	proc.Stop()

	// Stop servers with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Stop gRPC server
	if err := server.Stop(shutdownCtx); err != nil {
		logger.Error("Error during gRPC server shutdown", zap.Error(err))
	}

	// Stop metrics server
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("Error during metrics server shutdown", zap.Error(err))
	}

	// Log final stats
	epoch, lastLogID, lastTick, eventsReceived := proc.Stats()
	logger.Info("Final statistics",
		zap.Uint32("epoch", epoch),
		zap.Int64("lastLogID", lastLogID),
		zap.Uint32("lastTick", lastTick),
		zap.Uint64("eventsReceived", eventsReceived))

	logger.Info("Shutdown complete")
}

func epochsToStrings(epochs []uint32) []string {
	result := make([]string, len(epochs))
	for i, e := range epochs {
		result[i] = fmt.Sprintf("%d", e)
	}
	return result
}
