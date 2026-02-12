package domain

// use pointers to support empty numbers (by default omitempty treats zero values as empty)
type LogEventElastic struct {
	Epoch                 uint32 `json:"epoch"`
	TickNumber            uint64 `json:"tickNumber"`
	Timestamp             int64  `json:"timestamp"`
	EmittingContractIndex uint64 `json:"emittingContractIndex"`
	TransactionHash       string `json:"transactionHash,omitempty"`
	LogId                 uint64 `json:"logId"`
	LogDigest             string `json:"logDigest"`
	Type                  int16  `json:"type"` // short in elastic
	Category              *uint8 `json:"category,omitempty"`

	//Optional event body fields
	Source                 string  `json:"source,omitempty"`
	Destination            string  `json:"destination,omitempty"`
	Amount                 *uint64 `json:"amount,omitempty"`
	AssetName              string  `json:"assetName,omitempty"`
	AssetIssuer            string  `json:"assetIssuer,omitempty"`
	NumberOfShares         *uint64 `json:"numberOfShares,omitempty"`
	ManagingContractIndex  *uint64 `json:"managingContractIndex,omitempty"`
	UnitOfMeasurement      []byte  `json:"unitOfMeasurement,omitempty"`
	NumberOfDecimalPlaces  *byte   `json:"numberOfDecimalPlaces,omitempty"`
	DeductedAmount         *uint64 `json:"deductedAmount,omitempty"`
	RemainingAmount        *int64  `json:"remainingAmount,omitempty"`
	ContractIndex          *uint64 `json:"contractIndex,omitempty"`
	ContractIndexBurnedFor *uint64 `json:"contractIndexBurnedFor,omitempty"`
}

func (lee *LogEventElastic) IsSupported() bool {
	// qu transfer with zero amount is not supported
	ok := !(lee.Type == 0 && lee.EmittingContractIndex == 0 && *lee.Amount == 0)
	return ok
}
