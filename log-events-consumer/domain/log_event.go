package domain

import (
	"fmt"
	"slices"
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

// LogEvent data received from kafka that is already validated for missing fields.
// Some data types are a bit oversized but correspond with the elastic template.
// We do not care about the source data types but the target data types here.
type LogEvent struct {
	Epoch                 uint32         `json:"epoch"`
	TickNumber            uint64         `json:"tickNumber"`
	Index                 uint64         `json:"index"`
	Type                  int16          `json:"type"`
	EmittingContractIndex uint64         `json:"emittingContractIndex"`
	LogId                 uint64         `json:"logId"`
	LogDigest             string         `json:"logDigest"` // hex
	TransactionHash       string         `json:"transactionHash"`
	Timestamp             uint64         `json:"timestamp"`
	BodySize              uint32         `json:"bodySize"`
	Body                  map[string]any `json:"body"`
}

func (le *LogEvent) IsSupported(supportedMap map[uint64][]int16) bool {
	if supportedMap == nil {
		return false
	}
	logTypes, ok := supportedMap[le.EmittingContractIndex]
	if !ok {
		return false
	}
	return slices.Contains(logTypes, le.Type)
}

func (le *LogEvent) ToLogEventElastic() (LogEventElastic, error) {

	lee := LogEventElastic{
		Epoch:                 le.Epoch,
		TickNumber:            le.TickNumber,
		Timestamp:             le.Timestamp,
		EmittingContractIndex: le.EmittingContractIndex,
		TransactionHash:       le.TransactionHash,
		LogId:                 le.LogId,
		LogDigest:             le.LogDigest,
		Type:                  le.Type,
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
	// Disabled for the time being.
	/*if lee.Timestamp == 0 {
		invalid = append(invalid, "timestamp")
	}*/
	if lee.LogDigest == "" {
		invalid = append(invalid, "logDigest")
	}
	if len(invalid) > 0 {
		return LogEventElastic{}, fmt.Errorf("invalid zero value field(s): %v", invalid)
	}

	category, isCategorized, err := inferCategory(lee.TransactionHash)
	if err != nil {
		return LogEventElastic{}, fmt.Errorf("checking if transactionHash is special: %w", err)
	}
	if isCategorized {
		// We want to omit transactionHash field if tx is special, and instead set the category
		lee.TransactionHash = ""
		lee.Category = &category
	}

	switch {
	case lee.Type == 0:
		err = handleQuTransfer(&lee, le.Body)
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("handling qu transfer: %w", err)
		}

	case lee.Type == 1:
		err = handleAssetIssuance(&lee, le.Body)
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("handling asset issuance: %w", err)
		}

	case lee.Type == 2, lee.Type == 3:
		err = handleAssetTransfer(&lee, le.Body)
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("handling asset transfer: %w", err)
		}

	case lee.Type == 8:
		err = handleBurn(&lee, le.Body)
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("handling burn: %w", err)
		}

	case lee.Type == 13:
		err = handleReserveDeduction(&lee, le.Body)
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("handling reserve deduction: %w", err)
		}

	default:
		return LogEventElastic{}, fmt.Errorf("unsupported type: %v", lee.Type)

	}

	return lee, nil
}

func inferCategory(transactionHash string) (byte, bool, error) {

	if len(transactionHash) == 0 { // all good
		return 0, false, nil
	}

	err := validateIdentity(transactionHash, true)
	if err == nil { // all good
		return 0, false, nil
	}

	// check for encoded message in transaction hash

	if !strings.HasPrefix(transactionHash, "SC_") { // unknown encoding
		return 0, false, fmt.Errorf("unexpected special hash [%s]", transactionHash)
	}

	// Find the last underscore
	lastUnderscore := strings.LastIndex(transactionHash, "_") // find the last underscore to split
	if lastUnderscore == -1 {
		return 0, false, fmt.Errorf("cannot split special hash [%s]", transactionHash)
	}

	prefix := transactionHash[:lastUnderscore] // remove postfix
	value, ok := sysTransactionMap[prefix]     // find known category
	if !ok {
		return 0, false, fmt.Errorf("unexpected special event log type [%s]", transactionHash)
	}

	return value, true, nil
}

func handleQuTransfer(lee *LogEventElastic, body map[string]any) error {
	var err error
	source, ok := body["source"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid source")
	}
	lee.Source = source

	destination, ok := body["destination"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid destination")
	}
	lee.Destination = destination

	amount, ok := body["amount"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid amount")
	}
	lee.Amount, err = toUint64(amount)
	if err != nil {
		return fmt.Errorf("converting amount: %w", err)
	}

	return nil
}

func handleAssetIssuance(lee *LogEventElastic, body map[string]any) error {
	var err error

	issuer, ok := body["assetIssuer"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid asset issuer")
	}
	lee.AssetIssuer = issuer

	name, ok := body["assetName"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid asset name")
	}
	lee.AssetName = name

	mci, ok := body["managingContractIndex"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid managing contract index")
	}
	lee.ManagingContractIndex, err = toUint64(mci)
	if err != nil {
		return fmt.Errorf("converting managing contract index: %w", err)
	}

	ndp, ok := body["numberOfDecimalPlaces"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid number of decimal places")
	}
	lee.NumberOfDecimalPlaces, err = toByte(ndp)
	if err != nil {
		return fmt.Errorf("converting number of decimal places: %w", err)
	}

	nos, ok := body["numberOfShares"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid number of shares")
	}
	lee.NumberOfShares, err = toUint64(nos)
	if err != nil {
		return fmt.Errorf("converting number of shares: %w", err)
	}

	uom, ok := body["unitOfMeasurement"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid unit of measurement")
	}
	lee.UnitOfMeasurement, err = toUnitOfMeasurement(uom)
	if err != nil {
		return fmt.Errorf("converting unit of measurement: %w", err)
	}

	return nil
}

func handleAssetTransfer(lee *LogEventElastic, body map[string]any) error {
	var err error

	assetIssuer, ok := body["assetIssuer"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid asset issuer")
	}
	lee.AssetIssuer = assetIssuer

	assetName, ok := body["assetName"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid asset name")
	}
	lee.AssetName = assetName

	source, ok := body["source"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid source")
	}
	lee.Source = source

	destination, ok := body["destination"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid destination")
	}
	lee.Destination = destination

	nos, ok := body["numberOfShares"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid number of shares")
	}
	lee.NumberOfShares, err = toUint64(nos)
	if err != nil {
		return fmt.Errorf("converting number of shares: %w", err)
	}

	return nil
}

func handleBurn(lee *LogEventElastic, body map[string]any) error {
	var err error

	source, ok := body["source"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid source")
	}
	lee.Source = source

	amount, ok := body["amount"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid amount")
	}
	lee.Amount, err = toUint64(amount)
	if err != nil {
		return fmt.Errorf("converting amount: %w", err)
	}

	contractIndexBurnedFor, ok := body["contractIndexBurnedFor"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid contract index burned for")
	}
	lee.ContractIndexBurnedFor, err = toUint64(contractIndexBurnedFor)
	if err != nil {
		return fmt.Errorf("converting contract index burned for: %w", err)
	}

	return nil
}

func handleReserveDeduction(lee *LogEventElastic, body map[string]any) error {
	var err error

	ci, ok := body["contractIndex"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid contract index")
	}
	lee.ContractIndex, err = toUint64(ci)
	if err != nil {
		return fmt.Errorf("converting contract index: %w", err)
	}

	da, ok := body["deductedAmount"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid deducted amount")
	}
	lee.DeductedAmount, err = toUint64(da)
	if err != nil {
		return fmt.Errorf("converting deducted amount: %w", err)
	}

	ra, ok := body["remainingAmount"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid remaining amount")
	}
	lee.RemainingAmount, err = toInt64(ra)
	if err != nil {
		return fmt.Errorf("converting remaining amount: %w", err)
	}

	return nil
}

func toUnitOfMeasurement(unitOfMeasurement string) ([]byte, error) {
	if len(unitOfMeasurement) != 7 {
		err := fmt.Errorf("expected 7 characters but got [%s]", unitOfMeasurement)
		return nil, err
	}

	bytes := make([]byte, 7)
	for i := 0; i < 7; i++ {
		bytes[i] = unitOfMeasurement[i] - 48
	}
	return bytes, nil
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

func toUint64(num float64) (*uint64, error) {
	converted := uint64(num)
	if num < 0 || num != float64(converted) {
		return nil, fmt.Errorf("cannot convert to uint64: %f", num)
	}
	return &converted, nil
}

func toInt64(num float64) (*int64, error) {
	converted := int64(num)
	if num < 0 || num != float64(converted) {
		return nil, fmt.Errorf("cannot convert to int64: %f", num)
	}
	return &converted, nil
}

func toByte(num float64) (*byte, error) {
	if num < 0 || num > 255 {
		return nil, fmt.Errorf("cannot convert to byte: %f", num)
	}
	converted := byte(num)
	return &converted, nil
}

func If[T any](condition bool, trueValue, falseValue T) T {
	if condition {
		return trueValue
	}
	return falseValue
}
