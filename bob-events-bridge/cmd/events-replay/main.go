package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	grpcProm "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/qubic/bob-events-bridge/internal/config"
	bridgeGrpc "github.com/qubic/bob-events-bridge/internal/grpc"
	"github.com/qubic/bob-events-bridge/internal/kafka"
	"github.com/qubic/bob-events-bridge/internal/metrics"
	"github.com/qubic/bob-events-bridge/internal/storage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	googleGrpc "google.golang.org/grpc"
)

func main() {
	cfg, err := config.Parse()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse configuration: %v\n", err)
		os.Exit(1)
	}

	if err := validateReplayConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid replay configuration: %v\n", err)
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

	logger.Info("Starting bob-events-bridge REPLAY",
		zap.String("version", config.Version),
		zap.String("build", config.Build))

	promReg := prometheus.DefaultRegisterer

	grpcMetrics := grpcProm.NewServerMetrics(
		grpcProm.WithServerCounterOptions(grpcProm.WithConstLabels(prometheus.Labels{"namespace": cfg.Metrics.Namespace})),
	)
	promReg.MustRegister(grpcMetrics)

	bridgeMetrics := metrics.NewBridgeMetrics(promReg, cfg.Metrics.Namespace)

	// Initialize storage manager
	storageMgr, err := storage.NewManager(cfg.Storage.BasePath, cfg.Storage.KeepEpochs, logger)
	if err != nil {
		logger.Fatal("Failed to initialize storage", zap.Error(err))
	}
	defer storageMgr.Close() //nolint:errcheck

	logger.Info("Storage initialized",
		zap.Strings("epochs", epochsToStrings(storageMgr.GetAvailableEpochs())))

	if cfg.Replay.EnableServer {
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
	}

	if cfg.Replay.EnablePublish {
		db := storageMgr.GetEpochDB(uint32(cfg.Replay.Epoch))
		if db == nil {
			logger.Fatal("Replay epoch not found in storage",
				zap.Uint16("epoch", cfg.Replay.Epoch),
				zap.Strings("available", epochsToStrings(storageMgr.GetAvailableEpochs())))
		}

		brokers := strings.Split(cfg.Kafka.Brokers, ",")
		producer, err := kafka.NewProducer(
			brokers,
			cfg.Kafka.Topic,
			logger,
			bridgeMetrics,
			prometheus.DefaultRegisterer,
			prometheus.DefaultGatherer,
			cfg.Metrics.Namespace,
		)
		if err != nil {
			logger.Fatal("Failed to create Kafka producer", zap.Error(err))
		}
		defer producer.Close() //nolint:errcheck

		logger.Info("Kafka publisher enabled",
			zap.Strings("brokers", brokers),
			zap.String("topic", cfg.Kafka.Topic))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if _, err := runReplay(ctx, db, producer, cfg.Replay.TickStart, cfg.Replay.TickEnd, logger); err != nil {
			logger.Fatal("Replay failed", zap.Error(err))
		}
	}

	if cfg.Replay.EnableServer {
		// Block until interrupted; gRPC/HTTP keep serving.
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		logger.Info("Shutting down", zap.String("signal", sig.String()))
	}
}

// validateReplayConfig enforces flag combinations that must hold before any
// expensive setup runs. Fails fast with a clear message rather than failing
// later in storage/kafka init.
func validateReplayConfig(cfg *config.Config) error {
	if !cfg.Replay.EnableServer && !cfg.Replay.EnablePublish {
		return fmt.Errorf("at least one of Replay.EnableServer or Replay.EnablePublish must be true")
	}
	if cfg.Replay.EnablePublish {
		if !cfg.Kafka.Enabled {
			return fmt.Errorf("Replay.EnablePublish=true requires Kafka.Enabled=true")
		}
		if strings.TrimSpace(cfg.Kafka.Brokers) == "" {
			return fmt.Errorf("Replay.EnablePublish=true requires Kafka.Brokers to be set")
		}
	}
	if cfg.Replay.TickEnd != 0 && cfg.Replay.TickEnd < cfg.Replay.TickStart {
		return fmt.Errorf("Replay.TickEnd (%d) must be >= Replay.TickStart (%d)",
			cfg.Replay.TickEnd, cfg.Replay.TickStart)
	}
	return nil
}

func epochsToStrings(epochs []uint32) []string {
	result := make([]string, len(epochs))
	for i, e := range epochs {
		result[i] = fmt.Sprintf("%d", e)
	}
	return result
}
