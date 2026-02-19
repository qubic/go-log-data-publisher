# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

```bash
# Build
make build              # Builds binary to bin/events-bridge

# Run
make run                # Build and run
make run-debug          # Build and run with debug logging
./bin/events-bridge --help  # Show configuration options

# Test and lint
make test               # Run tests (go test -v ./...)
make lint               # Run golangci-lint
make fmt                # Format code

# Protobuf generation (requires buf CLI)
make proto              # Regenerate protobuf code from api/events-bridge/v1/
```

## Architecture Overview

This service bridges events from a Qubic bob node to clients via gRPC/REST. It connects to bob via WebSocket, receives blockchain events, stores them in PebbleDB (separated by epoch), and exposes them through gRPC with a REST gateway.

### Data Flow
```
Bob Node (WebSocket) → Processor → Storage Manager → Epoch DBs (PebbleDB)
                                                   ↓
                               gRPC/REST API ← Service Layer
```

### Key Components

**Processor** (`internal/processor/processor.go`): Core event loop that:
- Maintains WebSocket connection to bob with auto-reconnect
- Subscribes to log types (0: qu_transfer, 1: asset_issuance, 2: asset_ownership_change, 3: asset_possession_change, 8: burning, 13: contract_reserve_deduction)
- Handles crash recovery via lastTick-based resumption
- Deduplicates events before storage

**Storage Manager** (`internal/storage/manager.go`): Manages epoch-partitioned PebbleDB storage:
- Each epoch gets its own database at `data/bob-events-bridge/epoch_<N>/`
- Keys are formatted as `<tick_padded_10>:<event_id_padded_20>` for ordered iteration
- Persists state (epoch, lastLogID, lastTick) for crash recovery

**Bob WebSocket Client** (`internal/bob/websocket.go`): Protocol implementation for bob's /ws/qubic JSON-RPC 2.0 WebSocket API:
- Uses `qubic_subscribe` with `tickStream` for tick-based event streaming
- Receives all logs for a tick in one `qubic_subscription` notification
- Subscription with catch-up from startTick for crash recovery

**gRPC Service** (`internal/grpc/`): Exposes two endpoints:
- `GetStatus` (GET /v1/status): Returns epochs with tick intervals and event counts
- `GetEventsForTick` (POST /v1/getEventsForTick): Returns all events for a tick

### Configuration

Environment variables prefixed with `BOB_EVENTS_`:
- `BOB_EVENTS_BOB_WEBSOCKETURL` (default: `ws://localhost:40420/ws/qubic`)
- `BOB_EVENTS_BOB_LOGTYPES` (default: `0 1 2 3 8 13` - space-separated log type IDs)
- `BOB_EVENTS_STORAGE_BASEPATH` (default: `data/bob-events-bridge`)
- `BOB_EVENTS_SERVER_GRPCADDR` (default: `0.0.0.0:8001`)
- `BOB_EVENTS_SERVER_HTTPADDR` (default: `0.0.0.0:8000`)
- `BOB_EVENTS_BOB_OVERRIDESTARTTICK` (default: `false`)
- `BOB_EVENTS_BOB_STARTTICK` (default: `0`)
- `BOB_EVENTS_DEBUG` (default: `false`)

### Protobuf

Proto files are in `api/events-bridge/v1/`. The service uses grpc-gateway for REST endpoints. Run `make proto` after modifying `.proto` files (requires buf CLI).

## Bob WebSocket Event Format

### WebSocket Message Envelope

Each log event received from bob arrives as a JSON message with this structure:

```json
{
  "type": "log",
  "scIndex": 0,
  "logType": <uint32>,
  "isCatchUp": true|false,
  "message": {
    "ok": true,
    "epoch": <uint16>,
    "tick": <uint32>,
    "type": <uint8>,
    "logId": <uint64>,
    "logDigest": "<hex_string>",
    "bodySize": <uint32>,
    "logTypename": "<string>",
    "body": { ... },
    "timestamp": <uint64_unix_seconds>,
    "txHash": "<transaction_hash>"
  }
}
```

Events with `"ok": false` are skipped by the processor.

### Event Body by Log Type

**Type 0 — qu_transfer**
**bob format**
```json
{
  "from": "<qubic_address>",
  "to": "<qubic_address>",
  "amount": <int64>
}
```

**kafka format - from bob format <bob_{field}>**
```json
{
  "source": "<bob_from>",
  "destination": "<bob_to>",
  "amount": <int64>
}
```

**Type 1 — asset_issuance**
**bob format**
```json
{
  "issuerPublicKey": "<qubic_address>",
  "numberOfShares": <int64>,
  "managingContractIndex": <int64>,
  "name": "<string>",
  "numberOfDecimalPlaces": <int>,
  "unitOfMeasurement": "<string>"
}
```

**kafka format - from bob format <bob_{field}>**
```json
{
  "assetIssuer": "<bob_issuerPublicKey>",
  "numberOfShares": <int64>,
  "managingContractIndex": <int64>,
  "assetName": "<bob_name>",
  "numberOfDecimalPlaces": <int>,
  "unitOfMeasurement": "<string>"
}
```

**Type 2 — asset_ownership_change**
**bob format**
```json
{
  "sourcePublicKey": "<qubic_address>",
  "destinationPublicKey": "<qubic_address>",
  "issuerPublicKey": "<qubic_address>",
  "assetName": "<string>",
  "numberOfShares": <int64>
}
```

**kafka format - from bob format <bob_{field}>**
```json
{
  "source": "<bob_sourcePublicKey>",
  "destination": "<bob_destinationPublicKey>",
  "assetIssuer": "<bob_issuerPublicKey>",
  "assetName": "<string>",
  "numberOfShares": <int64>
}
```

**Type 3 — asset_possession_change**
**bob format**
```json
{
  "sourcePublicKey": "<qubic_address>",
  "destinationPublicKey": "<qubic_address>",
  "issuerPublicKey": "<qubic_address>",
  "assetName": "<string>",
  "numberOfShares": <int64>
}
```

**kafka format - from bob format <bob_{field}>**
```json
{
  "source": "<bob_sourcePublicKey>",
  "destination": "<bob_destinationPublicKey>",
  "assetIssuer": "<bob_issuerPublicKey>",
  "assetName": "<string>",
  "numberOfShares": <int64>
}
```

**Type 8 — burning**
**bob format**
```json
{
  "publicKey": "<qubic_address>",
  "amount": <int64>,
  "contractIndexBurnedFor": <uint32>
}
```

**kafka format - from bob format <bob_{field}>**
```json
{
  "source": "<bob_publicKey>",
  "amount": <int64>,
  "contractIndexBurnedFor": <uint32>
}
```

**Type 13 — contract_reserve_deduction**
**bob format**
```json
{
  "deductedAmount": <uint64>,
  "remainingAmount": <int64>,
  "contractIndex": <uint32>
}
```

**kafka format - from bob format <bob_{field}>**
```json
{
  "deductedAmount": <uint64>,
  "remainingAmount": <int64>,
  "contractIndex": <uint32>
}
```


**Kafka entire message format**
```json
{
  "index": <uint64>, // eveng log index created by use
  "emittingContractIndex": <scIndex from bob>,
  "type": <logType from bob>,
  "tickNumber": <tick from bob>,
  "epoch": 192,
  "logDigest": "a1b2c3d4e5f60718",
  "logId": <logId from bob>,
  "bodySize": 72,
  "timestamp": <uint64_unix_seconds_from_bob>,
  "transactionHash": <txHash from bob>,
  "body": {} // this is the event body which differs for each event type, the format is above for each format
}
```

All public keys/addresses are 60-character uppercase Qubic addresses.

## Development Notes

### Event Body Encoding

The Event proto uses `google.protobuf.Struct` for the body field to ensure proper JSON serialization in HTTP responses (nested JSON instead of base64-encoded bytes). This change is NOT wire-compatible with previously stored data - existing data stores must be deleted when schema changes are made.
