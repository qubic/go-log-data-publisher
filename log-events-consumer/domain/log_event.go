package domain

import (
	"fmt"
	"strings"
)

// Special system transactions
// Qubic supports, per tick, 1024 regular user transactions and a couple special transactions that SCs execute.
// In the core node code their index is defined as the NR_OF_TRANSACTIONS_PER_TICK + offset.
// In this code, they are mapped to their appropriate offset, which will be published to elastic if encountered.
var sysTransactionMap = map[string]byte{
	"SC_INITIALIZE_TX":   0x00,
	"SC_BEGIN_EPOCH_TX":  0x01,
	"SC_BEGIN_TICK_TX":   0x02,
	"SC_END_TICK_TX":     0x03,
	"SC_END_EPOCH_TX":    0x04,
	"SC_NOTIFICATION_TX": 0x05,
}

type LogEvent struct {
	Epoch                 uint32         `json:"epoch"`
	TickNumber            uint32         `json:"tickNumber"`
	Index                 uint64         `json:"index"`
	Type                  uint32         `json:"type"`
	EmittingContractIndex uint32         `json:"emittingContractIndex"`
	LogId                 uint64         `json:"logId"`
	LogDigest             string         `json:"logDigest"` // hex
	TransactionHash       string         `json:"transactionHash"`
	Timestamp             int64          `json:"timestamp"`
	BodySize              uint32         `json:"bodySize"`
	Body                  map[string]any `json:"body"`
}

// LogEventPtr mirrors LogEvent but uses pointers for all fields to detect presence during JSON unmarshalling.
type LogEventPtr struct {
	Epoch                 *uint32         `json:"epoch"`
	TickNumber            *uint32         `json:"tickNumber"`
	Index                 *uint64         `json:"index"`
	Type                  *uint32         `json:"type"`
	EmittingContractIndex *uint32         `json:"emittingContractIndex"`
	LogId                 *uint64         `json:"logId"`
	LogDigest             *string         `json:"logDigest"`
	TransactionHash       *string         `json:"transactionHash"`
	Timestamp             *int64          `json:"timestamp"`
	BodySize              *uint32         `json:"bodySize"`
	Body                  *map[string]any `json:"body"`
}

// ToLogEvent converts the pointer-based LogEventPtr into a concrete LogEvent.
// It validates presence (non-nil) of required fields and returns an error if any are missing.
func (lep LogEventPtr) ToLogEvent() (LogEvent, error) {
	missing := make([]string, 0, 6)
	if lep.Epoch == nil {
		missing = append(missing, "epoch")
	}
	if lep.TickNumber == nil {
		missing = append(missing, "tickNumber")
	}
	if lep.LogId == nil {
		missing = append(missing, "logId")
	}
	if lep.Timestamp == nil {
		missing = append(missing, "timestamp")
	}
	if lep.LogDigest == nil {
		missing = append(missing, "logDigest")
	}
	if lep.Index == nil {
		missing = append(missing, "index")
	}
	if lep.Type == nil {
		missing = append(missing, "type")
	}
	if lep.EmittingContractIndex == nil {
		missing = append(missing, "emittingContractIndex")
	}
	if len(missing) > 0 {
		return LogEvent{}, fmt.Errorf("missing required fields: %v", missing)
	}

	// Optional fields: if absent, use zero-values
	var hash string
	if lep.TransactionHash != nil {
		hash = *lep.TransactionHash
	}
	var bodySize uint32
	if lep.BodySize != nil {
		bodySize = *lep.BodySize
	}
	var body map[string]any
	if lep.Body != nil {
		body = *lep.Body
	}

	return LogEvent{
		Epoch:                 *lep.Epoch,
		TickNumber:            *lep.TickNumber,
		Index:                 *lep.Index,
		Type:                  *lep.Type,
		EmittingContractIndex: *lep.EmittingContractIndex,
		LogId:                 *lep.LogId,
		LogDigest:             *lep.LogDigest,
		TransactionHash:       hash,
		Timestamp:             *lep.Timestamp,
		BodySize:              bodySize,
		Body:                  body,
	}, nil
}

func (le *LogEvent) ToLogEventElastic() (LogEventElastic, error) {

	if le.Type > 32767 {
		return LogEventElastic{}, fmt.Errorf("type value %d exceeds int16 maximum of 32767", le.Type)
	}

	logEventElastic := LogEventElastic{
		Epoch:                 le.Epoch,
		TickNumber:            le.TickNumber,
		Timestamp:             le.Timestamp,
		EmittingContractIndex: le.EmittingContractIndex,
		TransactionHash:       le.TransactionHash,
		LogId:                 le.LogId,
		LogDigest:             le.LogDigest,
		Type:                  int16(le.Type),
	}

	txCategory, isSpecialTx, err := isSpecialSystemTransaction(logEventElastic.TransactionHash)
	if err != nil {
		return LogEventElastic{}, fmt.Errorf("checking if transactionHash is special: %w", err)
	}
	if isSpecialTx {
		// We want to omit transactionHash field if tx is special, and instead set the category
		logEventElastic.TransactionHash = ""
		logEventElastic.Category = &txCategory
	}

	for key, value := range le.Body {
		var err error
		switch key {
		case "source":
			err = assignTyped(key, value, &logEventElastic.Source)
		case "destination":
			err = assignTyped(key, value, &logEventElastic.Destination)
		case "amount":
			err = assignTyped(key, value, &logEventElastic.Amount)
			if err == nil && logEventElastic.Amount < 0 {
				err = fmt.Errorf("amount cannot be negative, got %d", logEventElastic.Amount)
			}
		case "assetName":
			err = assignTyped(key, value, &logEventElastic.AssetName)
		case "assetIssuer":
			err = assignTyped(key, value, &logEventElastic.AssetIssuer)
		case "numberOfShares":
			err = assignTyped(key, value, &logEventElastic.NumberOfShares)
			if err == nil && logEventElastic.NumberOfShares < 0 {
				err = fmt.Errorf("numberOfShares cannot be negative, got %d", logEventElastic.NumberOfShares)
			}
		case "managingContractIndex":
			err = assignTyped(key, value, &logEventElastic.ManagingContractIndex)
			if err == nil && logEventElastic.ManagingContractIndex < 0 {
				err = fmt.Errorf("managingContractIndex cannot be negative, got %d", logEventElastic.ManagingContractIndex)
			}
		case "unitOfMeasurement":
			if str, ok := value.(string); ok {
				if len(str) != 7 {
					err = fmt.Errorf("unitOfMeasurement must be exactly 7 characters, got %d", len(str))
				} else {
					bytes := make([]byte, 7)
					for i := 0; i < 7; i++ {
						bytes[i] = str[i] - 48
					}
					logEventElastic.UnitOfMeasurement = bytes
				}
			} else {
				err = fmt.Errorf("expected string for unitOfMeasurement, got %T", value)
			}

		case "numberOfDecimalPlaces":
			if num, ok := value.(float64); ok {
				if num != float64(int(num)) {
					err = fmt.Errorf("numberOfDecimalPlaces must be a whole number, got %f", num)
				} else if num < 0 || num > 255 {
					err = fmt.Errorf("numberOfDecimalPlaces must be in range 0-255, got %f", num)
				} else {
					logEventElastic.NumberOfDecimalPlaces = byte(num)
				}
			} else {
				err = fmt.Errorf("expected float64 for numberOfDecimalPlaces, got %T", value)
			}

		case "deductedAmount":
			err = assignTyped(key, value, &logEventElastic.DeductedAmount)
		case "remainingAmount":
			err = assignTyped(key, value, &logEventElastic.RemainingAmount)
		case "contractIndex":
			err = assignTyped(key, value, &logEventElastic.ContractIndex)
		case "contractIndexBurnedFor":
			err = assignTyped(key, value, &logEventElastic.ContractIndexBurnedFor)
		default:
			err = fmt.Errorf("unknown body key '%s', type: %T, value: %v", key, value, value)
		}
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("converting body field to proper type: %w", err)
		}
	}
	return logEventElastic, nil
}

type LogEventElastic struct {
	Epoch                 uint32 `json:"epoch"`
	TickNumber            uint32 `json:"tickNumber"`
	Timestamp             int64  `json:"timestamp"`
	EmittingContractIndex uint32 `json:"emittingContractIndex"`
	TransactionHash       string `json:"transactionHash,omitempty"`
	LogId                 uint64 `json:"logId"`
	LogDigest             string `json:"logDigest"`
	Type                  int16  `json:"type"`
	// This value is meant to be included in the message only if the transactionHash starts with one of the strings in
	// the sysTransactionMap map. In this event, transactionHash should no longer be included.
	// Important to use a byte pointer here.
	// This value may be set to 0 as in the event of an 'SC_INITIALIZE_TX', causing it to not be included in the elastic message.
	// Using a pointer here helps us because it can differentiate between not set (nil) and set with value 0.
	Category *byte `json:"category,omitempty"`

	//Optional event body fields
	Source                 string `json:"source,omitempty"`
	Destination            string `json:"destination,omitempty"`
	Amount                 int64  `json:"amount,omitempty"`
	AssetName              string `json:"assetName,omitempty"`
	AssetIssuer            string `json:"assetIssuer,omitempty"`
	NumberOfShares         int64  `json:"numberOfShares,omitempty"`
	ManagingContractIndex  int64  `json:"managingContractIndex,omitempty"`
	UnitOfMeasurement      []byte `json:"unitOfMeasurement,omitempty"`
	NumberOfDecimalPlaces  byte   `json:"numberOfDecimalPlaces,omitempty"`
	DeductedAmount         uint64 `json:"deductedAmount,omitempty"`
	RemainingAmount        int64  `json:"remainingAmount,omitempty"`
	ContractIndex          uint32 `json:"contractIndex,omitempty"`
	ContractIndexBurnedFor uint32 `json:"contractIndexBurnedFor,omitempty"`
}

func assignTyped[T any](key string, value any, target *T) error {
	// Try direct type match first
	if typed, ok := value.(T); ok {
		*target = typed
		return nil
	}

	// Handle JSON float64 to integer conversions
	if num, ok := value.(float64); ok {
		var zero T
		switch any(zero).(type) {
		case int64:
			if num != float64(int64(num)) {
				return fmt.Errorf("'%s' must be a whole number, got %f", key, num)
			}
			*target = any(int64(num)).(T)
			return nil
		case uint64:
			if num < 0 || num != float64(uint64(num)) {
				return fmt.Errorf("'%s' must be a non-negative whole number, got %f", key, num)
			}
			*target = any(uint64(num)).(T)
			return nil
		case uint32:
			if num < 0 || num != float64(uint32(num)) {
				return fmt.Errorf("'%s' must be a valid uint32, got %f", key, num)
			}
			*target = any(uint32(num)).(T)
			return nil
		}
	}

	var zero T
	return fmt.Errorf("wrong data type for '%s'. expected %T, got %T", key, zero, value)
}

func isSpecialSystemTransaction(transactionHash string) (byte, bool, error) {
	if !strings.HasPrefix(transactionHash, "SC_") {
		return 0x00, false, nil
	}

	// Find the last underscore
	lastUnderscore := strings.LastIndex(transactionHash, "_")
	if lastUnderscore == -1 {
		return 0x00, false, fmt.Errorf("cannot find last '_' character in transaction hash which starts with 'SC_' value: %s", transactionHash)
	}

	prefix := transactionHash[:lastUnderscore]
	value, ok := sysTransactionMap[prefix]
	if !ok {
		return 0x00, false, fmt.Errorf("unknown special system transaction '%s'", prefix)
	}

	return value, true, nil
}
