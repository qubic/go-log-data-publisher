package domain

import (
	"fmt"
	"strings"

	"github.com/qubic/go-node-connector/types"
)

// Special system transactions
// Qubic supports, per tick, 1024 regular user transactions and a couple special
// 'transactions' (special not transaction-related event logs).
// In the core node code their index is defined as the NR_OF_TRANSACTIONS_PER_TICK + offset.
// In this code, they are mapped to their appropriate offset, which will be published to elastic if encountered.
var sysTransactionMap = map[string]uint8{
	"SC_INITIALIZE_TX":   1,
	"SC_BEGIN_EPOCH_TX":  2,
	"SC_BEGIN_TICK_TX":   3,
	"SC_END_TICK_TX":     4,
	"SC_END_EPOCH_TX":    5,
	"SC_NOTIFICATION_TX": 6,
}

var supportedLogEventTypes = map[int16]struct{}{
	0:  {}, // qu transfer
	1:  {}, // asset issuance
	2:  {}, // asset ownership change
	3:  {}, // asset possession change
	8:  {}, // burn
	13: {}, // contract reserve deduction
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
// It validates the presence (non-nil) of required fields and returns an error if any are missing.
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

	if *lep.Epoch == 0 {
		missing = append(missing, "epoch")
	}
	if *lep.TickNumber == 0 {
		missing = append(missing, "tickNumber")
	}
	if *lep.LogId == 0 {
		missing = append(missing, "logId")
	}
	if *lep.LogDigest == "" {
		missing = append(missing, "logDigest")
	}
	if *lep.Timestamp == 0 {
		missing = append(missing, "timestamp")
	}

	if len(missing) > 0 {
		return LogEvent{}, fmt.Errorf("invalid zero value field(s): %v", missing)
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

func (le *LogEventElastic) IsSupported() bool {
	_, ok := supportedLogEventTypes[le.Type]
	// qu transfer with zero amount is not supported
	ok = ok && !(le.Type == 0 && le.EmittingContractIndex == 0 && le.Amount == 0)
	return ok
}

func (le *LogEvent) ToLogEventElastic() (LogEventElastic, error) {

	if le.Type > 32767 {
		return LogEventElastic{}, fmt.Errorf("type value %d exceeds int16 maximum of 32767", le.Type)
	}

	lee := LogEventElastic{
		Epoch:                 le.Epoch,
		TickNumber:            le.TickNumber,
		Timestamp:             le.Timestamp,
		EmittingContractIndex: le.EmittingContractIndex,
		TransactionHash:       le.TransactionHash,
		LogId:                 le.LogId,
		LogDigest:             le.LogDigest,
		Type:                  int16(le.Type),
	}

	// checking here again might be a bit paranoid
	invalid := make([]string, 0, 6)
	if lee.Epoch == 0 {
		invalid = append(invalid, "epoch")
	}
	if lee.TickNumber == 0 {
		invalid = append(invalid, "tickNumber")
	}
	if lee.LogId == 0 {
		invalid = append(invalid, "logId")
	}
	if lee.Timestamp == 0 {
		invalid = append(invalid, "timestamp")
	}
	if lee.LogDigest == "" {
		invalid = append(invalid, "logDigest")
	}
	if len(invalid) > 0 {
		return LogEventElastic{}, fmt.Errorf("invalid zero value field(s): %v", invalid)
	}

	eventLogCategory, isSpecialEventLog, err := inferCategoryFromTransactionHash(lee.TransactionHash)
	if err != nil {
		return LogEventElastic{}, fmt.Errorf("checking if transactionHash is special: %w", err)
	}
	if isSpecialEventLog {
		// We want to omit transactionHash field if tx is special, and instead set the category
		lee.TransactionHash = ""
		lee.Category = &eventLogCategory
	}

	switch {
	case lee.Type == 0:
	case lee.Type == 1:
	case lee.Type == 2:
	case lee.Type == 3:
	case lee.Type == 2:
	case lee.Type == 2:
	}

	// FIXME convert body values explicitly per type
	for key, value := range le.Body {
		var err error
		switch key {
		case "source":
			err = assignTyped(key, value, &lee.Source)
		case "destination":
			err = assignTyped(key, value, &lee.Destination)
		case "amount":
			err = assignTyped(key, value, &lee.Amount)
			if err == nil && lee.Amount < 0 {
				err = fmt.Errorf("amount cannot be negative, got %d", lee.Amount)
			}
		case "assetName":
			err = assignTyped(key, value, &lee.AssetName)
		case "assetIssuer":
			err = assignTyped(key, value, &lee.AssetIssuer)
		case "numberOfShares":
			err = assignTyped(key, value, &lee.NumberOfShares)
			if err == nil && lee.NumberOfShares < 0 {
				err = fmt.Errorf("numberOfShares cannot be negative, got %d", lee.NumberOfShares)
			}
		case "managingContractIndex":
			err = assignTyped(key, value, &lee.ManagingContractIndex)
			if err == nil && lee.ManagingContractIndex < 0 {
				err = fmt.Errorf("managingContractIndex cannot be negative, got %d", lee.ManagingContractIndex)
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
					lee.UnitOfMeasurement = bytes
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
					lee.NumberOfDecimalPlaces = byte(num)
				}
			} else {
				err = fmt.Errorf("expected float64 for numberOfDecimalPlaces, got %T", value)
			}

		case "deductedAmount":
			err = assignTyped(key, value, &lee.DeductedAmount)
		case "remainingAmount":
			err = assignTyped(key, value, &lee.RemainingAmount)
		case "contractIndex":
			err = assignTyped(key, value, &lee.ContractIndex)
		case "contractIndexBurnedFor":
			err = assignTyped(key, value, &lee.ContractIndexBurnedFor)
		default:
			err = fmt.Errorf("unknown body key '%s', type: %T, value: %v", key, value, value)
		}
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("converting body field to proper type: %w", err)
		}
	}
	return lee, nil
}

type LogEventElastic struct {
	Epoch                 uint32 `json:"epoch"`
	TickNumber            uint32 `json:"tickNumber"`
	Timestamp             int64  `json:"timestamp"`
	EmittingContractIndex uint32 `json:"emittingContractIndex"`
	TransactionHash       string `json:"transactionHash,omitempty"` // do not send, if empty!
	LogId                 uint64 `json:"logId"`
	LogDigest             string `json:"logDigest"`
	Type                  int16  `json:"type"` // short in elastic
	// This value is meant to be included in the message only if the transactionHash starts with one of the strings in
	// the sysTransactionMap map. In this event, transactionHash should no longer be included.
	// We use a pointer so that we can omit sending an (empty) value.
	Category *uint8 `json:"category,omitempty"` // do not send, if empty!

	//Optional event body fields
	Source                 string `json:"source,omitempty"`
	Destination            string `json:"destination,omitempty"`
	Amount                 uint64 `json:"amount,omitempty"`
	AssetName              string `json:"assetName,omitempty"`
	AssetIssuer            string `json:"assetIssuer,omitempty"`
	NumberOfShares         uint64 `json:"numberOfShares,omitempty"`
	ManagingContractIndex  uint64 `json:"managingContractIndex,omitempty"`
	UnitOfMeasurement      []byte `json:"unitOfMeasurement,omitempty"`
	NumberOfDecimalPlaces  byte   `json:"numberOfDecimalPlaces,omitempty"`
	DeductedAmount         uint64 `json:"deductedAmount,omitempty"`
	RemainingAmount        int64  `json:"remainingAmount,omitempty"`
	ContractIndex          uint64 `json:"contractIndex,omitempty"`
	ContractIndexBurnedFor uint64 `json:"contractIndexBurnedFor,omitempty"`
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
	return fmt.Errorf("wrong data type for '%s'. expected %T, got %T", key, zero, value) // unexpected type
}

func inferCategoryFromTransactionHash(transactionHash string) (byte, bool, error) {

	if len(transactionHash) == 0 { // all good
		return 0x00, false, nil
	}

	err := validateIdentity(transactionHash, true)
	if err == nil { // all good
		return 0x00, false, nil
	}

	// check for encoded message in transaction hash

	if !strings.HasPrefix(transactionHash, "SC_") { // unknown encoding
		return 0x00, false, fmt.Errorf("unexpected special hash [%s]", transactionHash)
	}

	// Find the last underscore
	lastUnderscore := strings.LastIndex(transactionHash, "_") // find the last underscore to split
	if lastUnderscore == -1 {
		return 0x00, false, fmt.Errorf("cannot split special hash [%s]", transactionHash)
	}

	prefix := transactionHash[:lastUnderscore] // remove postfix
	value, ok := sysTransactionMap[prefix]     // find known category
	if !ok {
		return 0x00, false, fmt.Errorf("unexpected special event log type [%s]", transactionHash)
	}

	return value, true, nil
}

func validateIdentity(digest string, isLowerCase bool) error {
	id := types.Identity(digest)
	pubKey, err := id.ToPubKey(isLowerCase)
	if err != nil {
		return fmt.Errorf("converting id to pubkey: %w", err)
	}

	var pubkeyFixed [32]byte
	copy(pubkeyFixed[:], pubKey[:32])
	id, err = id.FromPubKey(pubkeyFixed, isLowerCase)
	if err != nil {
		return fmt.Errorf("converting pubkey back to id: %w", err)
	}

	if id.String() != digest {
		return fmt.Errorf("invalid %s [%s]", If(isLowerCase, "hash", "identity"), digest)
	}
	return nil
}

func If[T any](condition bool, trueValue, falseValue T) T {
	if condition {
		return trueValue
	}
	return falseValue
}
