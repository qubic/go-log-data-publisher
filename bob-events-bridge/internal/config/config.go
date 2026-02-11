package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/ardanlabs/conf/v3"
)

// Build information set by ldflags
var (
	Build   = "develop"
	Version = "0.0.0"
)

// Config holds the application configuration
type Config struct {
	conf.Version
	Bob     BobConfig
	Storage StorageConfig
	Server  ServerConfig
	Kafka   KafkaConfig
	Debug   bool `conf:"default:false,help:enable debug logging"`
}

// BobConfig holds the bob node connection configuration
type BobConfig struct {
	WebSocketURL      string `conf:"default:ws://localhost:40420/ws/logs,help:bob WebSocket URL"`
	StatusURL         string `conf:"default:http://localhost:40420/status,help:bob status endpoint URL"`
	LogTypes          string `conf:"default:0 1 2 3 8 13,help:space-separated log types to subscribe to"`
	OverrideStartTick bool   `conf:"default:false,help:override persisted state and start from StartTick"`
	StartTick         uint32 `conf:"default:0,help:tick to start syncing from (requires OverrideStartTick)"`
}

// StorageConfig holds the storage configuration
type StorageConfig struct {
	BasePath string `conf:"default:data/bob-events-bridge,help:base path for data storage"`
}

// ServerConfig holds the server configuration
type ServerConfig struct {
	GRPCAddr string `conf:"default:0.0.0.0:8001,help:gRPC server address"`
	HTTPAddr string `conf:"default:0.0.0.0:8000,help:HTTP server address"`
}

// KafkaConfig holds the Kafka publisher configuration
type KafkaConfig struct {
	Brokers string `conf:"default:localhost:9092,help:comma-separated Kafka broker addresses"`
	Topic   string `conf:"default:qubic-events,help:Kafka topic name"`
	Enabled bool   `conf:"default:false,help:enable Kafka publishing"`
}

// SubscriptionEntry represents a single subscription
type SubscriptionEntry struct {
	SCIndex uint32
	LogType uint32
}

// Parse loads configuration from environment variables and CLI flags
func Parse() (*Config, error) {
	cfg := Config{
		Version: conf.Version{
			Build: Build,
			Desc:  "bob-events-bridge - Qubic events streaming service",
		},
	}

	help, err := conf.Parse("BOB_EVENTS", &cfg)
	if err != nil {
		if err == conf.ErrHelpWanted {
			fmt.Println(help)
			os.Exit(0)
		}
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	out, err := conf.String(&cfg)
	if err != nil {
		return nil, fmt.Errorf("generating config for output: %w", err)
	}
	log.Printf("main: Config :\n%v\n", out)

	return &cfg, nil
}

// GetSubscriptions parses the log types string into subscription entries
func (c *Config) GetSubscriptions() ([]SubscriptionEntry, error) {
	return ParseLogTypes(c.Bob.LogTypes)
}

// ParseLogTypes parses a space-separated string of log types
func ParseLogTypes(s string) ([]SubscriptionEntry, error) {
	if s == "" {
		return nil, nil
	}

	parts := strings.Fields(s)
	entries := make([]SubscriptionEntry, 0, len(parts))

	for _, part := range parts {
		logType, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid log type %q: %w", part, err)
		}

		entries = append(entries, SubscriptionEntry{
			SCIndex: 0, // Always core protocol
			LogType: uint32(logType),
		})
	}

	return entries, nil
}
