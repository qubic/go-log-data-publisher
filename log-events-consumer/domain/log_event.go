package domain

import (
	"fmt"
)

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

type LogEventElastic struct {
	Epoch                 uint32 `json:"epoch"`
	TickNumber            uint32 `json:"tickNumber"`
	Timestamp             int64  `json:"timestamp"`
	EmittingContractIndex uint32 `json:"emittingContractIndex"`
	TransactionHash       string `json:"transactionHash"`
	LogId                 uint64 `json:"logId"`
	LogDigest             string `json:"logDigest"`
	Type                  uint32 `json:"type"`

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

func LogEventToElastic(logEvent LogEvent) (LogEventElastic, error) {

	logEventElastic := LogEventElastic{
		Epoch:                 logEvent.Epoch,
		TickNumber:            logEvent.TickNumber,
		Timestamp:             logEvent.Timestamp,
		EmittingContractIndex: logEvent.EmittingContractIndex,
		TransactionHash:       logEvent.TransactionHash,
		LogId:                 logEvent.LogId,
		LogDigest:             logEvent.LogDigest,
		Type:                  logEvent.Type,
	}
	for key, value := range logEvent.Body {
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
