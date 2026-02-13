package bob

import (
	"encoding/json"
	"fmt"
)

// Log type constants
const (
	LogTypeQuTransfer               uint32 = 0
	LogTypeAssetIssuance            uint32 = 1
	LogTypeAssetOwnershipChange     uint32 = 2
	LogTypeAssetPossessionChange    uint32 = 3
	LogTypeBurning                  uint32 = 8
	LogTypeContractReserveDeduction uint32 = 13
)

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

// ParseEventBody unmarshals the raw body JSON into the typed struct for the
// given log type. Returns the typed struct as interface{}. Returns an error if
// the log type is unknown or the body is malformed.
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
	case LogTypeBurning:
		target = &BurningBody{}
	case LogTypeContractReserveDeduction:
		target = &ContractReserveDeductionBody{}
	default:
		return nil, fmt.Errorf("unknown log type: %d", logType)
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
