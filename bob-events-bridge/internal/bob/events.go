package bob

import (
	"encoding/json"
	"fmt"
)

// Log type constants
const (
	LogTypeQuTransfer                            uint32 = 0
	LogTypeAssetIssuance                         uint32 = 1
	LogTypeAssetOwnershipChange                  uint32 = 2
	LogTypeAssetPossessionChange                 uint32 = 3
	LogTypeContractErrorMessage                  uint32 = 4
	LogTypeContractWarningMessage                uint32 = 5
	LogTypeContractInformationMessage            uint32 = 6
	LogTypeContractDebugMessage                  uint32 = 7
	LogTypeBurning                               uint32 = 8
	LogTypeDustBurning                           uint32 = 9
	LogTypeSpectrumStats                         uint32 = 10
	LogTypeAssetOwnershipManagingContractChange  uint32 = 11
	LogTypeAssetPossessionManagingContractChange uint32 = 12
	LogTypeContractReserveDeduction              uint32 = 13
	LogTypeCustomMessage                         uint32 = 255
)

var LogTypeNames = map[uint32]string{
	LogTypeQuTransfer:                            "qu_transfer",
	LogTypeAssetIssuance:                         "asset_issuance",
	LogTypeAssetOwnershipChange:                  "asset_ownership_change",
	LogTypeAssetPossessionChange:                 "asset_possession_change",
	LogTypeContractErrorMessage:                  "contract_error_message",
	LogTypeContractWarningMessage:                "contract_warning_message",
	LogTypeContractInformationMessage:            "contract_information_message",
	LogTypeContractDebugMessage:                  "contract_debug_message",
	LogTypeBurning:                               "burning",
	LogTypeDustBurning:                           "dust_burning",
	LogTypeSpectrumStats:                         "spectrum_stats",
	LogTypeAssetOwnershipManagingContractChange:  "asset_ownership_managing_contract_change",
	LogTypeAssetPossessionManagingContractChange: "asset_possession_managing_contract_change",
	LogTypeContractReserveDeduction:              "contract_reserve_deduction",
	LogTypeCustomMessage:                         "custom_message",
}

// QuTransferBody represents the body of a qu_transfer event (log type 0)
type QuTransferBody struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Amount int64  `json:"amount"`
}

// AssetIssuanceBody represents the body of an asset_issuance event (log type 1)
type AssetIssuanceBody struct {
	IssuerPublicKey       string `json:"issuerPublicKey"`
	NumberOfShares        int64  `json:"numberOfShares"`
	ManagingContractIndex int64  `json:"managingContractIndex"`
	Name                  string `json:"name"`
	NumberOfDecimalPlaces int    `json:"numberOfDecimalPlaces"`
	UnitOfMeasurement     string `json:"unitOfMeasurement"`
}

// AssetOwnershipChangeBody represents the body of an asset_ownership_change event (log type 2)
type AssetOwnershipChangeBody struct {
	SourcePublicKey      string `json:"sourcePublicKey"`
	DestinationPublicKey string `json:"destinationPublicKey"`
	IssuerPublicKey      string `json:"issuerPublicKey"`
	AssetName            string `json:"assetName"`
	NumberOfShares       int64  `json:"numberOfShares"`
}

// AssetPossessionChangeBody represents the body of an asset_possession_change event (log type 3)
type AssetPossessionChangeBody struct {
	SourcePublicKey      string `json:"sourcePublicKey"`
	DestinationPublicKey string `json:"destinationPublicKey"`
	IssuerPublicKey      string `json:"issuerPublicKey"`
	AssetName            string `json:"assetName"`
	NumberOfShares       int64  `json:"numberOfShares"`
}

// BurningBody represents the body of a burning event (log type 8)
type BurningBody struct {
	PublicKey              string `json:"publicKey"`
	Amount                 int64  `json:"amount"`
	ContractIndexBurnedFor uint32 `json:"contractIndexBurnedFor"`
}

// ContractReserveDeductionBody represents the body of a contract_reserve_deduction event (log type 13)
type ContractReserveDeductionBody struct {
	DeductedAmount  uint64 `json:"deductedAmount"`
	RemainingAmount int64  `json:"remainingAmount"`
	ContractIndex   uint32 `json:"contractIndex"`
}

// ContractMessageBody represents the body of contract message events (log types 4-7)
type ContractMessageBody struct {
	SCIndex   uint32 `json:"scIndex"`
	SCLogType uint32 `json:"scLogType"`
	Content   string `json:"content"`
}

// HexBody represents the body of hex-encoded events (log types 9, 10, 11, 12)
type HexBody struct {
	Hex string `json:"hex"`
}

// CustomMessageBody represents the body of a custom_message event (log type 255)
type CustomMessageBody struct {
	CustomMessage string `json:"customMessage"`
}

// ParseEventBody unmarshals the raw body JSON into the typed struct for the
// given log type. Returns the typed struct as interface{}. Unknown log types
// are parsed as a generic map[string]any so they pass through without error.
func ParseEventBody(logType uint32, body json.RawMessage) (interface{}, error) {
	if len(body) == 0 {
		return nil, nil
	}

	var target interface{}
	switch logType {
	case LogTypeQuTransfer:
		target = &QuTransferBody{}
	case LogTypeAssetIssuance:
		target = &AssetIssuanceBody{}
	case LogTypeAssetOwnershipChange:
		target = &AssetOwnershipChangeBody{}
	case LogTypeAssetPossessionChange:
		target = &AssetPossessionChangeBody{}
	case LogTypeContractErrorMessage, LogTypeContractWarningMessage,
		LogTypeContractInformationMessage, LogTypeContractDebugMessage:
		target = &ContractMessageBody{}
	case LogTypeBurning:
		target = &BurningBody{}
	case LogTypeDustBurning, LogTypeSpectrumStats,
		LogTypeAssetOwnershipManagingContractChange, LogTypeAssetPossessionManagingContractChange:
		target = &HexBody{}
	case LogTypeContractReserveDeduction:
		target = &ContractReserveDeductionBody{}
	case LogTypeCustomMessage:
		target = &CustomMessageBody{}
	default:
		// Unknown log type — pass through the raw body as a generic map
		var m map[string]interface{}
		if err := json.Unmarshal(body, &m); err != nil {
			return nil, fmt.Errorf("failed to unmarshal body for unknown log type %d: %w", logType, err)
		}
		return m, nil
	}

	if err := json.Unmarshal(body, target); err != nil {
		return nil, fmt.Errorf("failed to unmarshal body for log type %d: %w", logType, err)
	}

	return target, nil
}

// EventBodyToMap converts a typed event body struct to map[string]interface{}
// by marshaling to JSON and unmarshaling back into a map.
func EventBodyToMap(body interface{}) (map[string]interface{}, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal body to JSON: %w", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal body to map: %w", err)
	}

	return result, nil
}

func GetLogTypeName(eventType uint32) string {
	name, ok := LogTypeNames[eventType]
	if !ok {
		return "unknown"
	}
	return name
}
