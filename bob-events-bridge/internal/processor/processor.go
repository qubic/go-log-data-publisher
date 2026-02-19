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
	"github.com/qubic/bob-events-bridge/internal/kafka"
	"github.com/qubic/bob-events-bridge/internal/metrics"
	"github.com/qubic/bob-events-bridge/internal/storage"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
)

// tickBatch accumulates events for a single tick before flushing
type tickBatch struct {
	tick        uint32
	epoch       uint32
	kafkaMsgs   []*kafka.EventMessage
	protoEvents []*eventsbridge.Event
	lastLogID   uint64
}

// Processor handles connecting to bob and processing events
type Processor struct {
	cfg           *config.Config
	subscriptions []config.SubscriptionEntry
	storage       *storage.Manager
	logger        *zap.Logger
	client        *bob.WSClient
	publisher     kafka.Publisher // nil if Kafka disabled
	metrics       *metrics.BridgeMetrics

	mu             sync.RWMutex
	running        bool
	currentEpoch   uint32
	lastLogID      int64
	lastTick       uint32
	eventsReceived uint64
	tickEventIndex uint32
	tickForIndex   uint32 // The tick that tickEventIndex is valid for

	pendingBatch *tickBatch
}

// NewProcessor creates a new event processor
func NewProcessor(cfg *config.Config, subscriptions []config.SubscriptionEntry, storage *storage.Manager, logger *zap.Logger, publisher kafka.Publisher, metrics *metrics.BridgeMetrics) *Processor {
	return &Processor{
		cfg:           cfg,
		subscriptions: subscriptions,
		storage:       storage,
		logger:        logger,
		publisher:     publisher,
		metrics:       metrics,
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
		p.metrics.SetCurrentEpoch(uint16(state.CurrentEpoch))
		p.lastLogID = state.LastLogID
		p.lastTick = state.LastTick
		p.logger.Info("Resuming from saved state",
			zap.Uint32("epoch", p.currentEpoch),
			zap.Int64("lastLogID", p.lastLogID),
			zap.Uint32("lastTick", p.lastTick))

		if state.LastTick > 0 {
			count, err := p.storage.CountEventsForTick(state.CurrentEpoch, state.LastTick)
			if err != nil {
				p.logger.Warn("Failed to count events for tick, starting index at 0",
					zap.Uint32("tick", state.LastTick),
					zap.Error(err))
			} else {
				p.tickEventIndex = count
				p.tickForIndex = state.LastTick
				p.logger.Info("Recovered tick event index",
					zap.Uint32("tick", state.LastTick),
					zap.Uint32("tickEventIndex", p.tickEventIndex))
			}
		}
	} else {
		// Clean start — fetch initialTick from bob status endpoint
		status, err := bob.FetchStatus(ctx, p.cfg.Bob.StatusURL)
		if err != nil {
			return fmt.Errorf("failed to fetch bob status for initial tick: %w", err)
		}
		p.lastTick = status.InitialTick - 1
		p.logger.Info("Starting fresh from bob's initial tick",
			zap.Uint32("initialTick", p.lastTick))
	}

	// Override start tick if configured (overrides both persisted state and fresh start)
	if p.cfg.Bob.OverrideStartTick {
		p.lastTick = p.cfg.Bob.StartTick - 1
		p.lastLogID = 0
		p.tickEventIndex = 0
		p.logger.Info("Overriding start tick from config",
			zap.Uint32("startTick", p.lastTick))
	}

	p.mu.Lock()
	p.running = true
	p.mu.Unlock()

	p.metrics.SetProcessorRunning(true)

	// Run the main processing loop with reconnection logic
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Context cancelled, stopping processor")
			p.metrics.SetProcessorRunning(false)
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
				p.metrics.IncProcessorReconnections()
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

	p.metrics.SetProcessorRunning(false)

	if p.client != nil {
		_ = p.client.Close()
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
		p.metrics.SetCurrentEpoch(welcome.CurrentEpoch)
	}

	// Build subscription list from config
	subscriptions := make([]bob.SubscriptionEntry, len(p.subscriptions))
	for i, sub := range p.subscriptions {
		subscriptions[i] = bob.SubscriptionEntry{
			SCIndex: sub.SCIndex,
			LogType: sub.LogType,
		}
	}

	// Subscribe with lastTick for crash recovery (tick-based resumption only)
	var lastTickPtr *uint32
	if p.lastTick > 0 {
		lastTickPtr = &p.lastTick
	}

	if err := p.client.Subscribe(subscriptions, lastTickPtr); err != nil {
		_ = p.client.Close()
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
			if err := p.flushBatch(ctx); err != nil {
				p.logger.Warn("Failed to flush batch on context cancellation", zap.Error(err))
			}
			return ctx.Err()
		default:
		}

		msg, err := p.client.ReadMessage()
		if err != nil {
			p.client.SetConnected(false)
			if flushErr := p.flushBatch(context.Background()); flushErr != nil {
				p.logger.Warn("Failed to flush batch on disconnect", zap.Error(flushErr))
			}
			return fmt.Errorf("read error: %w", err)
		}

		if err := p.handleMessage(ctx, msg); err != nil {
			p.logger.Error("Failed to handle message", zap.Error(err), zap.ByteString("rawMessage", msg))
			return fmt.Errorf("fatal message handling error: %w", err)
		}
	}
}

// handleMessage processes a single message from bob
func (p *Processor) handleMessage(ctx context.Context, data []byte) error {
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
		return p.handleLogMessage(ctx, data)

	case bob.MessageTypeCatchUpComplete:
		if err := p.flushBatch(ctx); err != nil {
			return fmt.Errorf("failed to flush batch on catch-up complete: %w", err)
		}

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
		var pong bob.PongMessage
		if err := json.Unmarshal(data, &pong); err == nil {
			p.mu.RLock()
			lastTick := p.lastTick
			p.mu.RUnlock()
			p.metrics.SetProcessingDelta(int64(pong.ServerTick) - int64(lastTick))
		}

	case bob.MessageTypeError:
		var msg bob.ErrorMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			return fmt.Errorf("failed to parse error message: %w", err)
		}
		p.logger.Error("Received error from bob",
			zap.String("code", msg.Code),
			zap.String("message", msg.Message))
		p.metrics.IncProcessorBobErrors(msg.Code)

	default:
		p.logger.Debug("Received unknown message type", zap.String("type", base.Type))
	}

	return nil
}

// flushBatch publishes and stores the pending batch, then resets it.
// On failure the batch is discarded — the processor will disconnect and
// bob will resend from lastTick on reconnect (at-least-once).
func (p *Processor) flushBatch(ctx context.Context) error {
	if p.pendingBatch == nil || len(p.pendingBatch.protoEvents) == 0 {
		p.pendingBatch = nil
		return nil
	}

	batch := p.pendingBatch
	p.pendingBatch = nil

	// Publish to Kafka before storage (at-least-once delivery)
	if p.publisher != nil && len(batch.kafkaMsgs) > 0 {
		if err := p.publisher.PublishEvents(ctx, batch.kafkaMsgs); err != nil {
			for _, event := range batch.protoEvents {
				p.metrics.IncProcessorEventsFailed(event.EventType, "kafka_error")
			}
			return fmt.Errorf("failed to publish batch to kafka: %w", err)
		}
	}

	// Batch PebbleDB write
	if err := p.storage.StoreEvents(batch.protoEvents); err != nil {
		for _, event := range batch.protoEvents {
			p.metrics.IncProcessorEventsFailed(event.EventType, "storage_error")
		}
		return fmt.Errorf("failed to store event batch: %w", err)
	}

	for _, event := range batch.protoEvents {
		p.metrics.IncProcessorEventsProcessed(event.EventType)
	}

	// Update local state
	p.mu.Lock()
	p.lastLogID = int64(batch.lastLogID)
	p.lastTick = batch.tick
	p.eventsReceived += uint64(len(batch.protoEvents))
	p.mu.Unlock()

	p.metrics.SetLastProcessedTick(batch.tick)
	p.metrics.SetLastProcessedLogID(batch.lastLogID)
	p.metrics.SetCurrentTickEventCount(0)

	p.logger.Debug("Flushed batch",
		zap.Uint32("tick", batch.tick),
		zap.Uint32("epoch", batch.epoch),
		zap.Int("events", len(batch.protoEvents)))

	return nil
}

// handleLogMessage processes a log event
func (p *Processor) handleLogMessage(ctx context.Context, data []byte) error {
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
		p.metrics.IncProcessorEventsSkippedNonOK()
		return nil
	}

	p.metrics.IncProcessorEventsReceived(payload.Type)

	// Check for epoch transition — flush before changing epoch
	if uint32(payload.Epoch) != p.currentEpoch {
		if err := p.flushBatch(ctx); err != nil {
			return fmt.Errorf("failed to flush batch on epoch transition: %w", err)
		}
		p.logger.Info("Epoch transition detected",
			zap.Uint32("oldEpoch", p.currentEpoch),
			zap.Uint16("newEpoch", payload.Epoch))
		p.currentEpoch = uint32(payload.Epoch)
		p.metrics.SetCurrentEpoch(payload.Epoch)
	}

	// Tick boundary — flush if tick changed
	if p.pendingBatch != nil && payload.Tick != p.pendingBatch.tick {
		if err := p.flushBatch(ctx); err != nil {
			return fmt.Errorf("failed to flush batch on tick change: %w", err)
		}
		p.tickEventIndex = 0
		p.tickForIndex = payload.Tick
	}

	// Reset index if processing a different tick than what tickEventIndex was recovered/set for.
	// This handles the case where pendingBatch is nil (e.g., all events for the previous tick
	// were deduplicated) but we're now processing a new tick.
	if p.tickForIndex != 0 && p.tickForIndex != payload.Tick {
		p.tickEventIndex = 0
		p.tickForIndex = payload.Tick
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
		p.metrics.IncProcessorEventsDeduplicated(payload.Type)
		return nil
	}

	// Validate and parse body into typed struct
	parsed, err := bob.ParseEventBody(payload.Type, payload.Body)
	if err != nil {
		p.metrics.IncProcessorEventsFailed(payload.Type, "parse_error")
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

	// Build kafka message (if publisher configured)
	var kafkaMsg *kafka.EventMessage
	if p.publisher != nil {
		kafkaMsg, err = kafka.BuildEventMessage(&logMsg, &payload, parsed, p.tickEventIndex)
		if err != nil {
			return fmt.Errorf("failed to build kafka message: %w", err)
		}
	}

	// Create event proto
	event := &eventsbridge.Event{
		LogId:       payload.LogID,
		Tick:        payload.Tick,
		Epoch:       uint32(payload.Epoch),
		EventType:   payload.Type,
		TxHash:      payload.TxHash,
		Timestamp:   payload.Timestamp,
		Body:        bodyStruct,
		IndexInTick: p.tickEventIndex,
		LogDigest:   payload.LogDigest,
	}

	// Initialize batch if nil
	if p.pendingBatch == nil {
		p.pendingBatch = &tickBatch{
			tick:  payload.Tick,
			epoch: uint32(payload.Epoch),
		}
		// Track which tick the index is for
		if p.tickForIndex == 0 {
			p.tickForIndex = payload.Tick
		}
	}

	// Add to batch
	if kafkaMsg != nil {
		p.pendingBatch.kafkaMsgs = append(p.pendingBatch.kafkaMsgs, kafkaMsg)
	}
	p.pendingBatch.protoEvents = append(p.pendingBatch.protoEvents, event)
	p.pendingBatch.lastLogID = payload.LogID

	p.tickEventIndex++

	p.metrics.SetCurrentTickEventCount(len(p.pendingBatch.protoEvents))

	p.logger.Debug("Buffered event",
		zap.Uint64("logID", payload.LogID),
		zap.Uint32("tick", payload.Tick),
		zap.Uint16("epoch", payload.Epoch),
		zap.Uint32("type", payload.Type),
		zap.Uint32("scIndex", logMsg.SCIndex),
		zap.Uint32("logType", logMsg.LogType),
		zap.Bool("isCatchUp", logMsg.IsCatchUp),
		zap.Bool("kafkaEnabled", p.publisher != nil),
		zap.Int("batchSize", len(p.pendingBatch.protoEvents)))

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
