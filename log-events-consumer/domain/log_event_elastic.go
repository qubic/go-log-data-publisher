package domain

// LogEventElastic document sent to elastic.
// Uses pointers to support potential zeros (by default, omitempty treats zero values as empty)
// Make sure to add json e2e tests for every supported log event.
type LogEventElastic struct {
	Epoch      uint32 `json:"epoch"`
	TickNumber uint64 `json:"tickNumber"`
	Timestamp  uint64 `json:"timestamp"`

	TransactionHash string `json:"transactionHash,omitempty"`
	LogId           uint64 `json:"logId"`
	LogDigest       string `json:"logDigest"`
	Type            int16  `json:"type"` // short in elastic
	Category        *uint8 `json:"category,omitempty"`

	//Optional event body fields
	EmittingContractIndex    *uint64 `json:"emittingContractIndex,omitempty"`
	Source                   string  `json:"source,omitempty"`
	Destination              string  `json:"destination,omitempty"`
	Owner                    string  `json:"owner,omitempty"`
	Possessor                string  `json:"possessor,omitempty"`
	Amount                   *uint64 `json:"amount,omitempty"`
	AssetName                string  `json:"assetName,omitempty"`
	AssetIssuer              string  `json:"assetIssuer,omitempty"`
	NumberOfShares           *uint64 `json:"numberOfShares,omitempty"`
	ManagingContractIndex    *uint64 `json:"managingContractIndex,omitempty"`
	UnitOfMeasurement        []byte  `json:"unitOfMeasurement,omitempty"`
	NumberOfDecimalPlaces    *byte   `json:"numberOfDecimalPlaces,omitempty"`
	DeductedAmount           *uint64 `json:"deductedAmount,omitempty"`
	RemainingAmount          *int64  `json:"remainingAmount,omitempty"`
	ContractIndex            *uint64 `json:"contractIndex,omitempty"`
	ContractIndexBurnedFor   *uint64 `json:"contractIndexBurnedFor,omitempty"`
	SourceContractIndex      *uint64 `json:"sourceContractIndex,omitempty"`
	DestinationContractIndex *uint64 `json:"destinationContractIndex,omitempty"`
	CustomMessage            *uint64 `json:"customMessage,omitempty"`
	ContractMessageType      *uint64 `json:"contractMessageType,omitempty"`
	RawPayload               []byte  `json:"rawPayload,omitempty"`
}

func (lee *LogEventElastic) IsSupported() bool {

	// qu transfer with zero amount is not supported
	if lee.Type == 0 && *lee.Amount == 0 {
		return false
	}

	// burn events with zero amount are not supported
	if lee.Type == 8 && *lee.Amount == 0 {
		return false
	}

	return true
}
