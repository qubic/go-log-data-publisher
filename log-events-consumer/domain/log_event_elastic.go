package domain

import "encoding/json"

// Categories is a custom type to ensure JSON marshaling as array of numbers, not base64
type Categories []uint8

// MarshalJSON implements custom JSON marshaling to prevent base64 encoding
func (c Categories) MarshalJSON() ([]byte, error) {
	if c == nil {
		return []byte("null"), nil
	}
	// Convert to []int to force array marshaling instead of base64
	nums := make([]int, len(c))
	for i, v := range c {
		nums[i] = int(v)
	}
	return json.Marshal(nums)
}

// LogEventElastic document sent to elastic.
// Uses pointers to support potential zeros (by default, omitempty treats zero values as empty)
// Make sure to add json e2e tests for every supported log event.
type LogEventElastic struct {
	Epoch      uint32 `json:"epoch"`
	TickNumber uint64 `json:"tickNumber"`
	Timestamp  uint64 `json:"timestamp"`

	TransactionHash string     `json:"transactionHash,omitempty"`
	LogId           uint64     `json:"logId"`
	LogDigest       string     `json:"logDigest"`
	LogType         int16      `json:"logType"` // short in elastic
	Categories      Categories `json:"categories,omitempty"`

	//Optional event body fields
	ContractIndex            *uint64 `json:"contractIndex,omitempty"`
	ContractMessageType      *uint64 `json:"contractMessageType,omitempty"`
	Source                   string  `json:"source,omitempty"`
	Destination              string  `json:"destination,omitempty"`
	Amount                   *uint64 `json:"amount,omitempty"`
	AssetName                string  `json:"assetName,omitempty"`
	AssetIssuer              string  `json:"assetIssuer,omitempty"`
	NumberOfShares           *uint64 `json:"numberOfShares,omitempty"`
	ManagingContractIndex    *uint64 `json:"managingContractIndex,omitempty"`
	UnitOfMeasurement        []byte  `json:"unitOfMeasurement,omitempty"`
	NumberOfDecimalPlaces    *byte   `json:"numberOfDecimalPlaces,omitempty"`
	DeductedAmount           *uint64 `json:"deductedAmount,omitempty"`
	RemainingAmount          *int64  `json:"remainingAmount,omitempty"`
	Owner                    string  `json:"owner,omitempty"`
	Possessor                string  `json:"possessor,omitempty"`
	SourceContractIndex      *uint64 `json:"sourceContractIndex,omitempty"`
	DestinationContractIndex *uint64 `json:"destinationContractIndex,omitempty"`
	CustomMessage            *uint64 `json:"customMessage,omitempty"`
	RawPayload               []byte  `json:"rawPayload,omitempty"`
	QueryingEntity           string  `json:"queryingEntity,omitempty"`
	QueryID                  *uint64 `json:"queryId,omitempty"`
	QueryType                *int16  `json:"queryType,omitempty"`
	QueryStatus              *int16  `json:"queryStatus,omitempty"`
	SubscriptionID           *uint32 `json:"subscriptionId,omitempty"`
	InterfaceIndex           *uint64 `json:"interfaceIndex,omitempty"`
	PeriodMillis             *uint64 `json:"periodMillis,omitempty"`
	FirstQueryTimestamp      *uint64 `json:"firstQueryTimestamp,omitempty"`
}

func (lee *LogEventElastic) IsSupported() bool {

	// qu transfer with zero amount is not supported
	if lee.LogType == 0 && *lee.Amount == 0 {
		return false
	}

	// burn events with zero amount are not supported
	if lee.LogType == 8 && *lee.Amount == 0 {
		return false
	}

	return true
}
