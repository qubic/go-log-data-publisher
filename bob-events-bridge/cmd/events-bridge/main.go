package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/qubic/bob-events-bridge/internal/config"
	"github.com/qubic/bob-events-bridge/internal/grpc"
	"github.com/qubic/bob-events-bridge/internal/kafka"
	"github.com/qubic/bob-events-bridge/internal/processor"
	"github.com/qubic/bob-events-bridge/internal/storage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
	defer logger.Sync()

	logger.Info("Starting bob-events-bridge",
		zap.String("version", config.Version),
		zap.String("build", config.Build))

	logger.Info("Configuration loaded",
		zap.String("bobWebSocketURL", cfg.Bob.WebSocketURL),
		zap.String("storagePath", cfg.Storage.BasePath),
		zap.String("grpcAddr", cfg.Server.GRPCAddr),
		zap.String("httpAddr", cfg.Server.HTTPAddr))

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
	defer storageMgr.Close()

	logger.Info("Storage initialized",
		zap.Strings("epochs", epochsToStrings(storageMgr.GetAvailableEpochs())))

	// Create and start gRPC server
	server := grpc.NewServer(storageMgr, logger)
	if err := server.Start(grpc.ServerConfig{
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
		kafkaPublisher = kafka.NewProducer(brokers, cfg.Kafka.Topic, logger)
		defer kafkaPublisher.Close()
		logger.Info("Kafka publisher enabled",
			zap.Strings("brokers", brokers),
			zap.String("topic", cfg.Kafka.Topic))
	}

	// Create processor
	proc := processor.NewProcessor(cfg, subscriptions, storageMgr, logger, kafkaPublisher)

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start processor in goroutine
	processorDone := make(chan error, 1)
	go func() {
		processorDone <- proc.Start(ctx)
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

	if err := server.Stop(shutdownCtx); err != nil {
		logger.Error("Error during server shutdown", zap.Error(err))
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
