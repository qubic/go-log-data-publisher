package grpc

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	eventsbridge "github.com/qubic/bob-events-bridge/api/events-bridge/v1"
	"github.com/qubic/bob-events-bridge/internal/storage"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
)

// ServerConfig holds server configuration
type ServerConfig struct {
	GRPCAddr       string
	HTTPAddr       string
	MaxRecvMsgSize int
	MaxSendMsgSize int
}

// Server manages the gRPC and HTTP gateway servers
type Server struct {
	service      *EventsBridgeService
	grpcServer   *grpc.Server
	httpServer   *http.Server
	grpcListener net.Listener
	logger       *zap.Logger
	extraOpts    []grpc.ServerOption
}

// NewServer creates a new server instance
func NewServer(storage *storage.Manager, logger *zap.Logger, extraOpts ...grpc.ServerOption) *Server {
	return &Server{
		service:   NewEventsBridgeService(storage, logger),
		logger:    logger,
		extraOpts: extraOpts,
	}
}

// Start starts both gRPC and HTTP servers
func (s *Server) Start(cfg ServerConfig) error {
	// Set defaults
	if cfg.MaxRecvMsgSize == 0 {
		cfg.MaxRecvMsgSize = 4 * 1024 * 1024 // 4MB
	}
	if cfg.MaxSendMsgSize == 0 {
		cfg.MaxSendMsgSize = 100 * 1024 * 1024 // 100MB
	}

	// Create gRPC server
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(cfg.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(cfg.MaxSendMsgSize),
	}
	opts = append(opts, s.extraOpts...)
	s.grpcServer = grpc.NewServer(opts...)

	// Register service
	eventsbridge.RegisterEventsBridgeServiceServer(s.grpcServer, s.service)
	reflection.Register(s.grpcServer)

	// Start gRPC listener
	var err error
	s.grpcListener, err = net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on gRPC port: %w", err)
	}

	// Start gRPC server in goroutine
	go func() {
		s.logger.Info("Starting gRPC server", zap.String("addr", cfg.GRPCAddr))
		if err := s.grpcServer.Serve(s.grpcListener); err != nil {
			s.logger.Error("gRPC server error", zap.Error(err))
		}
	}()

	// Start HTTP gateway if address is specified
	if cfg.HTTPAddr != "" {
		if err := s.startHTTPGateway(cfg); err != nil {
			s.grpcServer.Stop()
			return fmt.Errorf("failed to start HTTP gateway: %w", err)
		}
	}

	return nil
}

// startHTTPGateway starts the grpc-gateway HTTP server
func (s *Server) startHTTPGateway(cfg ServerConfig) error {
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions: protojson.MarshalOptions{
				EmitDefaultValues: true,
				EmitUnpopulated:   true,
			},
		}),
	)

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(cfg.MaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(cfg.MaxSendMsgSize),
		),
	}

	if err := eventsbridge.RegisterEventsBridgeServiceHandlerFromEndpoint(
		context.Background(),
		mux,
		cfg.GRPCAddr,
		opts,
	); err != nil {
		return fmt.Errorf("failed to register HTTP handler: %w", err)
	}

	s.httpServer = &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: mux,
	}

	go func() {
		s.logger.Info("Starting HTTP gateway", zap.String("addr", cfg.HTTPAddr))
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("HTTP server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop gracefully stops both servers
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("Stopping servers")

	// Stop HTTP server first
	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			s.logger.Warn("HTTP server shutdown error", zap.Error(err))
		}
	}

	// Stop gRPC server
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	return nil
}

// GRPCAddr returns the gRPC listen address
func (s *Server) GRPCAddr() net.Addr {
	if s.grpcListener != nil {
		return s.grpcListener.Addr()
	}
	return nil
}
