package grpc

import (
	"context"

	eventsbridge "github.com/qubic/bob-events-bridge/api/events-bridge/v1"
	"github.com/qubic/bob-events-bridge/internal/storage"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// EventsBridgeService implements the gRPC service
type EventsBridgeService struct {
	eventsbridge.UnimplementedEventsBridgeServiceServer
	storage *storage.Manager
	logger  *zap.Logger
}

// NewEventsBridgeService creates a new service instance
func NewEventsBridgeService(storage *storage.Manager, logger *zap.Logger) *EventsBridgeService {
	return &EventsBridgeService{
		storage: storage,
		logger:  logger,
	}
}

// GetStatus returns the current service status
func (s *EventsBridgeService) GetStatus(ctx context.Context, _ *emptypb.Empty) (*eventsbridge.GetStatusResponse, error) {
	response, err := s.storage.GetStatus()
	if err != nil {
		s.logger.Error("Failed to get status", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to get status: %v", err)
	}

	return response, nil
}

// GetEventsForTick returns all events for a specific tick
func (s *EventsBridgeService) GetEventsForTick(ctx context.Context, req *eventsbridge.GetEventsForTickRequest) (*eventsbridge.GetEventsForTickResponse, error) {
	if req.Tick == 0 {
		return nil, status.Error(codes.InvalidArgument, "tick must be greater than 0")
	}

	epoch, events, err := s.storage.GetEventsForTick(req.Tick)
	if err != nil {
		s.logger.Error("Failed to get events for tick",
			zap.Uint32("tick", req.Tick),
			zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to get events: %v", err)
	}

	return &eventsbridge.GetEventsForTickResponse{
		Tick:   req.Tick,
		Epoch:  epoch,
		Events: events,
	}, nil
}
