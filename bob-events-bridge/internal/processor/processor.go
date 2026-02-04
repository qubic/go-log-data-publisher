package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	eventsbridge "github.com/qubic/bob-events-bridge/api/events-bridge/v1"
	"github.com/qubic/bob-events-bridge/internal/bob"
	"github.com/qubic/bob-events-bridge/internal/config"
	"github.com/qubic/bob-events-bridge/internal/storage"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
)

// Processor handles connecting to bob and processing events
type Processor struct {
	cfg           *config.Config
	subscriptions []config.SubscriptionEntry
	storage       *storage.Manager
	logger        *zap.Logger
	client        *bob.WSClient

	mu             sync.RWMutex
	running        bool
	currentEpoch   uint32
	lastLogID      int64
	lastTick       uint32
	eventsReceived uint64
}

// NewProcessor creates a new event processor
func NewProcessor(cfg *config.Config, subscriptions []config.SubscriptionEntry, storage *storage.Manager, logger *zap.Logger) *Processor {
	return &Processor{
		cfg:           cfg,
		subscriptions: subscriptions,
		storage:       storage,
		logger:        logger,
	}
}

// Start begins the event processing loop
func (p *Processor) Start(ctx context.Context) error {
	// Load state for crash recovery
	state, err := p.storage.LoadState()
	if err != nil {
		return fmt.Errorf("failed to load state: %w", err)
	}

	if state.HasState {
		p.currentEpoch = state.CurrentEpoch
		p.lastLogID = state.LastLogID
		p.lastTick = state.LastTick
		p.logger.Info("Resuming from saved state",
			zap.Uint32("epoch", p.currentEpoch),
			zap.Int64("lastLogID", p.lastLogID),
			zap.Uint32("lastTick", p.lastTick))
	} else {
		p.logger.Info("Starting fresh, no previous state found")
	}

	p.mu.Lock()
	p.running = true
	p.mu.Unlock()

	// Run the main processing loop with reconnection logic
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Context cancelled, stopping processor")
			return ctx.Err()
		default:
		}

		if err := p.connectAndProcess(ctx); err != nil {
			p.logger.Error("Processing error, will reconnect",
				zap.Error(err))

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
				continue
			}
		}
	}
}

// Stop stops the processor
func (p *Processor) Stop() {
	p.mu.Lock()
	p.running = false
	p.mu.Unlock()

	if p.client != nil {
		p.client.Close()
	}
}

// connectAndProcess handles a single connection session
func (p *Processor) connectAndProcess(ctx context.Context) error {
	// Create WebSocket client
	p.client = bob.NewWSClient(p.cfg.Bob.WebSocketURL, p.logger)

	// Connect to bob
	welcome, err := p.client.Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	p.logger.Info("Connected to bob",
		zap.Uint32("currentVerifiedTick", welcome.CurrentVerifiedTick),
		zap.Uint16("currentEpoch", welcome.CurrentEpoch))

	// Update current epoch if this is a fresh start
	if p.currentEpoch == 0 {
		p.currentEpoch = uint32(welcome.CurrentEpoch)
	}

	// Build subscription list from config
	subscriptions := make([]bob.SubscriptionEntry, len(p.subscriptions))
	for i, sub := range p.subscriptions {
		subscriptions[i] = bob.SubscriptionEntry{
			SCIndex: sub.SCIndex,
			LogType: sub.LogType,
		}
	}

	// Subscribe with both lastLogID and lastTick for crash recovery
	var lastLogIDPtr *int64
	var lastTickPtr *uint32
	if p.lastLogID > 0 {
		lastLogIDPtr = &p.lastLogID
	}
	if p.lastTick > 0 {
		lastTickPtr = &p.lastTick
	}

	if err := p.client.Subscribe(subscriptions, lastLogIDPtr, lastTickPtr); err != nil {
		p.client.Close()
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	// Start ping goroutine
	pingCtx, pingCancel := context.WithCancel(ctx)
	defer pingCancel()
	go p.pingLoop(pingCtx)

	// Process messages
	return p.processMessages(ctx)
}

// pingLoop sends periodic pings to keep the connection alive
func (p *Processor) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.client != nil && p.client.IsConnected() {
				if err := p.client.SendPing(); err != nil {
					p.logger.Warn("Failed to send ping", zap.Error(err))
				}
			}
		}
	}
}

// processMessages reads and processes messages from the WebSocket
func (p *Processor) processMessages(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := p.client.ReadMessage()
		if err != nil {
			p.client.SetConnected(false)
			return fmt.Errorf("read error: %w", err)
		}

		if err := p.handleMessage(msg); err != nil {
			p.logger.Error("Failed to handle message", zap.Error(err))
			return fmt.Errorf("fatal message handling error: %w", err)
		}
	}
}

// handleMessage processes a single message from bob
func (p *Processor) handleMessage(data []byte) error {
	var base bob.BaseMessage
	if err := json.Unmarshal(data, &base); err != nil {
		return fmt.Errorf("failed to parse base message: %w", err)
	}

	// Debug: log all message types for troubleshooting
	if base.Type != bob.MessageTypeLog && base.Type != bob.MessageTypePong {
		p.logger.Debug("Received message", zap.String("type", base.Type), zap.ByteString("raw", data))
	}

	switch base.Type {
	case bob.MessageTypeLog:
		return p.handleLogMessage(data)

	case bob.MessageTypeCatchUpComplete:
		var msg bob.CatchUpCompleteMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			return fmt.Errorf("failed to parse catchup message: %w", err)
		}
		p.logger.Info("Catch-up complete",
			zap.Int("logsDelivered", msg.LogsDelivered),
			zap.Int64("fromLogID", msg.FromLogID),
			zap.Int64("toLogID", msg.ToLogID))

	case bob.MessageTypeAck:
		var msg bob.AckMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			return fmt.Errorf("failed to parse ack message: %w", err)
		}
		p.logger.Debug("Received ack",
			zap.String("action", msg.Action),
			zap.Bool("success", msg.Success),
			zap.Uint32("scIndex", msg.SCIndex),
			zap.Uint32("logType", msg.LogType),
			zap.Int("subscriptionsAdded", msg.SubscriptionsAdded))

	case bob.MessageTypePong:
		p.logger.Debug("Received pong")

	case bob.MessageTypeError:
		var msg bob.ErrorMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			return fmt.Errorf("failed to parse error message: %w", err)
		}
		p.logger.Error("Received error from bob",
			zap.String("code", msg.Code),
			zap.String("message", msg.Message))

	default:
		p.logger.Debug("Received unknown message type", zap.String("type", base.Type))
	}

	return nil
}

// handleLogMessage processes a log event
func (p *Processor) handleLogMessage(data []byte) error {
	var logMsg bob.LogMessage
	if err := json.Unmarshal(data, &logMsg); err != nil {
		return fmt.Errorf("failed to parse log message: %w", err)
	}

	// Parse the payload
	var payload bob.LogPayload
	if err := json.Unmarshal(logMsg.Message, &payload); err != nil {
		return fmt.Errorf("failed to parse log payload: %w", err)
	}

	if !payload.OK {
		p.logger.Debug("Skipping non-OK log message")
		return nil
	}

	// Check for epoch transition
	if uint32(payload.Epoch) != p.currentEpoch {
		p.logger.Info("Epoch transition detected",
			zap.Uint32("oldEpoch", p.currentEpoch),
			zap.Uint16("newEpoch", payload.Epoch))
		p.currentEpoch = uint32(payload.Epoch)
	}

	// Deduplication check: skip if we already have this event
	exists, err := p.storage.HasEvent(p.currentEpoch, payload.Tick, payload.LogID)
	if err != nil {
		p.logger.Warn("Failed to check for duplicate event", zap.Error(err))
		// Continue processing - better to risk a duplicate than to skip
	} else if exists {
		p.logger.Debug("Skipping duplicate event",
			zap.Uint64("logID", payload.LogID),
			zap.Uint32("tick", payload.Tick),
			zap.Uint32("fromLogType", logMsg.LogType))
		return nil
	}

	// Validate and parse body into typed struct
	parsed, err := bob.ParseEventBody(payload.Type, payload.Body)
	if err != nil {
		return fmt.Errorf("failed to parse event body for log type %d: %w", payload.Type, err)
	}

	// Convert typed struct to protobuf Struct
	var bodyStruct *structpb.Struct
	if parsed != nil {
		bodyMap, err := bob.EventBodyToMap(parsed)
		if err != nil {
			return fmt.Errorf("failed to convert event body to map: %w", err)
		}
		bodyStruct, err = structpb.NewStruct(bodyMap)
		if err != nil {
			return fmt.Errorf("failed to convert body to protobuf Struct: %w", err)
		}
	}

	// Create event proto
	event := &eventsbridge.Event{
		LogId:     payload.LogID,
		Tick:      payload.Tick,
		Epoch:     uint32(payload.Epoch),
		EventType: payload.Type,
		TxHash:    payload.TxHash,
		Timestamp: payload.Timestamp,
		Body:      bodyStruct,
	}

	// Store the event
	if err := p.storage.StoreEvent(event); err != nil {
		return fmt.Errorf("failed to store event: %w", err)
	}

	// Update local state
	p.mu.Lock()
	p.lastLogID = int64(payload.LogID)
	p.lastTick = payload.Tick
	p.eventsReceived++
	p.mu.Unlock()

	p.logger.Debug("Stored event",
		zap.Uint64("logID", payload.LogID),
		zap.Uint32("tick", payload.Tick),
		zap.Uint16("epoch", payload.Epoch),
		zap.Uint32("type", payload.Type),
		zap.Uint32("scIndex", logMsg.SCIndex),
		zap.Uint32("logType", logMsg.LogType),
		zap.Bool("isCatchUp", logMsg.IsCatchUp))

	return nil
}

// Stats returns current processing statistics
func (p *Processor) Stats() (epoch uint32, lastLogID int64, lastTick uint32, eventsReceived uint64) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentEpoch, p.lastLogID, p.lastTick, p.eventsReceived
}

// IsRunning returns whether the processor is running
func (p *Processor) IsRunning() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.running
}
