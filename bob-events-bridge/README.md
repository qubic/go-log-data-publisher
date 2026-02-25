# bob-events-bridge

A Go service that bridges events from a Qubic bob blockchain node to client applications via gRPC and REST APIs.

## Overview

bob-events-bridge connects to a bob node via WebSocket, ingests real-time blockchain events, stores them in epoch-partitioned databases, and exposes them through a dual gRPC/REST API. The service is designed for reliability with automatic crash recovery, event deduplication, and seamless epoch transitions.

## Features

- **Real-time event streaming** from bob node via WebSocket
- **Epoch-partitioned storage** using PebbleDB for efficient data management
- **Crash recovery** with automatic resumption from last processed tick
- **Event deduplication** to prevent duplicate processing
- **Dual API interface** with native gRPC and REST gateway
- **Multi-epoch support** with automatic database creation for new epochs
- **Configurable log types** subscription (qu_transfer, asset_issuance, etc.)

## Architecture

```
Bob Node (WebSocket)
    │
    ▼ (JSON messages)
┌─────────────────────────────────────────┐
│              Processor                   │
│  - WebSocket connection management       │
│  - Automatic reconnection                │
│  - Event deduplication                   │
│  - Epoch transition handling             │
└─────────────────────────────────────────┘
    │
    ▼ (Event structs)
┌─────────────────────────────────────────┐
│           Storage Manager                │
│  - Epoch database lifecycle              │
│  - State persistence                     │
│  - Tick range queries                    │
└─────────────────────────────────────────┘
    │
    ▼ (PebbleDB writes)
┌─────────────────────────────────────────┐
│    Epoch DBs        │    State DB       │
│  epochs/{0,1,2}/    │    state/         │
└─────────────────────────────────────────┘
    │
    ▼ (Queries)
┌─────────────────────────────────────────┐
│            gRPC Service                  │
│  - GetStatus                             │
│  - GetEventsForTick                      │
└─────────────────────────────────────────┘
    │
    ├──► gRPC (port 8001)
    │
    └──► REST Gateway (port 8000)
```

## Prerequisites

- Go 1.23 or later
- Access to a running bob node
- [buf CLI](https://buf.build/docs/installation) (for protobuf regeneration only)
- [golangci-lint](https://golangci-lint.run/welcome/install/) (for linting only)

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd bob-events-bridge

# Build the binary
make build

# The binary is output to bin/events-bridge
```

## Configuration

Configuration is loaded from environment variables with the `BOB_EVENTS_` prefix:

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `BOB_EVENTS_BOB_WEBSOCKETURL` | `ws://localhost:40420/ws/logs` | Bob node WebSocket endpoint |
| `BOB_EVENTS_BOB_STATUSURL` | `http://localhost:40420/status` | Bob node status endpoint |
| `BOB_EVENTS_BOB_LOGTYPES` | `0 1 2 3` | Space-separated log types to subscribe |
| `BOB_EVENTS_STORAGE_BASEPATH` | `data/bob-events-bridge` | Base path for database storage |
| `BOB_EVENTS_SERVER_GRPCADDR` | `0.0.0.0:8001` | gRPC server listen address |
| `BOB_EVENTS_SERVER_HTTPADDR` | `0.0.0.0:8000` | HTTP/REST server listen address |
| `BOB_EVENTS_DEBUG` | `false` | Enable debug logging |

### Log Types

| Type | Description |
|------|-------------|
| 0 | qu_transfer |
| 1 | asset_issuance |
| 2 | asset_ownership_change |
| 3 | asset_possession_change |

## Running

```bash
# Run with default configuration
make run

# Run with debug logging
make run-debug

# Run with custom configuration
BOB_EVENTS_BOB_WEBSOCKETURL=ws://bob-node:40420/ws/logs \
BOB_EVENTS_SERVER_HTTPADDR=0.0.0.0:9000 \
./bin/events-bridge

# Show configuration help
./bin/events-bridge --help
```

## API Reference

The service exposes a gRPC API with an automatic REST gateway.

### GetStatus

Returns the current service status including epoch information and processing state.

**gRPC:** `EventsBridgeService.GetStatus`
**REST:** `GET /v1/status`

**Response:**
```json
{
  "epochs": [
    {
      "epoch": 145,
      "intervals": [
        {"firstTick": 22001, "lastTick": 22500}
      ],
      "totalEvents": 1523
    }
  ],
  "currentEpoch": 145,
  "lastProcessedTick": 22500,
  "lastProcessedLogId": "4821"
}
```

### GetEventsForTick

Retrieves all events for a specific tick.

**gRPC:** `EventsBridgeService.GetEventsForTick`
**REST:** `POST /v1/getEventsForTick`

**Request:**
```json
{
  "tick": 22001
}
```

**Response:**
```json
{
  "tick": 22001,
  "epoch": 145,
  "events": [
    {
      "logId": "1",
      "tick": 22001,
      "epoch": 145,
      "eventType": 0,
      "txHash": "abc123...",
      "timestamp": "2024-01-15T10:30:00Z",
      "body": {
        "sourceId": "SENDER...",
        "destId": "RECEIVER...",
        "amount": "1000000"
      }
    }
  ]
}
```

### Example Usage

```bash
# Get service status
curl http://localhost:8000/v1/status

# Get events for a specific tick
curl -X POST http://localhost:8000/v1/getEventsForTick \
  -H "Content-Type: application/json" \
  -d '{"tick": 22001}'

# Using grpcurl
grpcurl -plaintext localhost:8001 events_bridge.v1.EventsBridgeService/GetStatus
```

## Storage Structure

```
data/bob-events-bridge/
├── state/                    # Crash recovery state
│   ├── current_epoch
│   ├── last_log_id
│   └── last_tick
└── epochs/
    ├── 145/                  # Epoch 145 events
    │   └── (PebbleDB files)
    ├── 146/                  # Epoch 146 events
    │   └── (PebbleDB files)
    └── ...
```

Events are keyed by `{tick}:{logId}` (both zero-padded) enabling efficient range queries by tick.

## Development

### Make Targets

```bash
make build      # Build the binary
make run        # Build and run
make run-debug  # Build and run with debug logging
make test       # Run all tests
make fmt        # Format code
make lint       # Run linter
make proto      # Regenerate protobuf code
```

### Regenerating Protobuf Code

After modifying `.proto` files:

```bash
cd api/events-bridge/v1
buf generate
```

### Running Tests

```bash
# Run all tests
make test

# Run specific test
go test -v ./internal/storage/... -run TestEpochMigration

# Run e2e tests
go test -v ./internal/e2e/...
```

## Crash Recovery

The service implements automatic crash recovery:

1. **State persistence** - After processing each event, the service persists:
   - Current epoch
   - Last processed log ID
   - Last processed tick

2. **Resumption** - On restart, the service:
   - Loads the persisted state
   - Subscribes to bob with the `lastTick` parameter
   - Bob resumes delivery from the specified tick
   - Client-side deduplication prevents reprocessing

3. **Deduplication** - Events are deduplicated using:
   - In-memory tracking during processing
   - Database existence checks for cross-session duplicates

## Connection Management

- **Automatic reconnection** with 5-second backoff on connection loss
- **Application-level pings** every 30 seconds to detect stale connections
- **Read timeouts** trigger reconnection to recover from silent failures
- **Graceful shutdown** with configurable timeout

## Kafka Message Format

Every event published to Kafka follows this envelope structure:

```json
{
  "index": 0,
  "type": 0,
  "tickNumber": 22000001,
  "epoch": 145,
  "logDigest": "a1b2c3d4e5f60718",
  "logId": 42,
  "bodySize": 72,
  "timestamp": 1718461800,
  "transactionHash": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  "body": {}
}
```

| Field | Type | Description |
|-------|------|-------------|
| `index` | uint64 | Zero-based index of the event within its tick |
| `type` | uint32 | Log type identifier (see body formats below) |
| `tickNumber` | uint32 | Tick number the event belongs to |
| `epoch` | uint32 | Epoch number |
| `logDigest` | string | Hex-encoded digest of the log entry |
| `logId` | uint64 | Unique log ID from bob |
| `bodySize` | uint32 | Size of the original body in bytes |
| `timestamp` | uint64 | Unix timestamp in seconds |
| `transactionHash` | string | 60-character Qubic transaction hash |
| `body` | object | Event-specific payload (see below) |

### Body by Log Type

#### Type 0 — qu_transfer

```json
{
  "source": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  "destination": "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
  "amount": 1000
}
```

#### Type 1 — asset_issuance

```json
{
  "assetIssuer": "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  "numberOfShares": 1000000,
  "managingContractIndex": 5,
  "assetName": "QX",
  "numberOfDecimalPlaces": 0,
  "unitOfMeasurement": "shares"
}
```

#### Type 2 — asset_ownership_change

```json
{
  "source": "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  "destination": "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  "assetIssuer": "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  "assetName": "QX",
  "numberOfShares": 200
}
```

#### Type 3 — asset_possession_change

```json
{
  "source": "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  "destination": "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  "assetIssuer": "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  "assetName": "CFB",
  "numberOfShares": 300
}
```

#### Types 4–7 — contract messages (error / warning / information / debug)

```json
{
  "scIndex": 1,
  "scLogType": 42,
  "content": "error: something failed"
}
```

#### Type 8 — burning

```json
{
  "source": "BURNAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  "amount": 999,
  "contractIndexBurnedFor": 7
}
```

#### Types 9–12 — hex-encoded events (dust_burning, spectrum_stats, asset_ownership_managing_contract_change, asset_possession_managing_contract_change)

```json
{
  "hex": "deadbeef0123456789abcdef"
}
```

#### Type 13 — contract_reserve_deduction

```json
{
  "deductedAmount": 5000,
  "remainingAmount": 95000,
  "contractIndex": 3
}
```

#### Type 255 — custom_message

```json
{
  "customMessage": "12345"
}
```

#### Unknown types (default)

Any log type not listed above is parsed as a hex body:

```json
{
  "hex": "1f129599cc4910bb4d..."
}
```

## License

[Add license information]
