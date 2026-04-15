package domain

import (
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

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
	Epoch           uint32         `json:"epoch"`
	TickNumber      uint64         `json:"tickNumber"`
	Index           uint64         `json:"index"`
	Type            int16          `json:"type"`
	LogId           uint64         `json:"logId"`
	LogDigest       string         `json:"logDigest"` // hex
	TransactionHash string         `json:"transactionHash"`
	Timestamp       uint64         `json:"timestamp"`
	BodySize        uint32         `json:"bodySize"`
	Body            map[string]any `json:"body"`
	LastLogForTick  bool           `json:"lastLogForTick"`
}

func (le *LogEvent) IsSupported(supportedMap map[uint64][]int16) bool {
	if supportedMap == nil {
		return false
	}
	logTypes, ok := supportedMap[0] // check for main event type only here
	if !ok {
		return false
	}
	return slices.Contains(logTypes, le.Type)
}

func (le *LogEvent) ToLogEventElastic() (LogEventElastic, error) {

	lee := LogEventElastic{
		Epoch:           le.Epoch,
		TickNumber:      le.TickNumber,
		TransactionHash: le.TransactionHash,
		LogId:           le.LogId,
		LogDigest:       le.LogDigest,
		LogType:         le.Type,
	}

	if le.Timestamp > 10000000000 { // year 2286 in seconds
		return LogEventElastic{}, fmt.Errorf("invalid seconds timestamp (probably already in ms): %d", le.Timestamp)
	}
	lee.Timestamp = le.Timestamp * 1000 // source is in seconds

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

	category, isCategorized, err := inferCategory(lee.TransactionHash)
	if err != nil {
		return LogEventElastic{}, fmt.Errorf("checking if transactionHash is special: %w", err)
	}
	if isCategorized {
		// We want to omit transactionHash field if tx is special, and instead set the category
		lee.TransactionHash = ""
		lee.Categories = Categories{category}
	}

	switch lee.LogType {
	case 0:
		err = handleQuTransfer(&lee, le.Body)
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("handling qu transfer: %w", err)
		}

	case 1:
		err = handleAssetIssuance(&lee, le.Body)
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("handling asset issuance: %w", err)
		}

	case 2, 3:
		err = handleAssetTransfer(&lee, le.Body)
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("handling asset transfer: %w", err)
		}

	case 4, 5, 6, 7:
		err = handleSmartContractMessage(&lee, le.Body)
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("handling smart contract message with type [%d]: %w", lee.LogType, err)
		}

	case 8:
		err = handleBurn(&lee, le.Body)
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("handling burn: %w", err)
		}

	case 9, 10: // raw payload only
		err = handleRaw(&lee, le.Body)
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("handling raw event with type [%d]: %w", lee.LogType, err)
		}

	case 11, 12:
		isPossessionTransfer := lee.LogType == 12
		err = handleAssetManagementTransfer(&lee, le.Body, isPossessionTransfer)
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("handling asset management transfer: %w", err)
		}

	case 13:
		err = handleReserveDeduction(&lee, le.Body)
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("handling reserve deduction: %w", err)
		}

	case 14:
		err = handleOracleQueryStatusChange(&lee, le.Body)
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("handling oracle query status change: %w", err)
		}

	case 15:
		err = handleOracleSubscriberLogMessage(&lee, le.Body)
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("handling oracle subscriber log message: %w", err)
		}

	case 255:
		err = handleCustomMessage(&lee, le.Body)
		if err != nil {
			return LogEventElastic{}, fmt.Errorf("handling custom message: %w", err)
		}

	default:
		return LogEventElastic{}, fmt.Errorf("unsupported type: %v", lee.LogType)

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

func handleAssetManagementTransfer(lee *LogEventElastic, body map[string]any, isPossessionTransfer bool) error {
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

	nos, ok := body["numberOfShares"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid number of shares")
	}
	lee.NumberOfShares, err = toUint64(nos)
	if err != nil {
		return fmt.Errorf("converting number of shares: %w", err)
	}

	sourceContractIndex, ok := body["sourceContractIndex"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid source contract index")
	}
	lee.SourceContractIndex, err = toUint64(sourceContractIndex)
	if err != nil {
		return fmt.Errorf("converting source contract index: %w", err)
	}

	destinationContractIndex, ok := body["destinationContractIndex"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid destination contract index")
	}
	lee.DestinationContractIndex, err = toUint64(destinationContractIndex)
	if err != nil {
		return fmt.Errorf("converting destination contract index: %w", err)
	}

	owner, ok := body["owner"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid owner public key")
	}
	lee.Owner = owner

	if isPossessionTransfer {
		possessor, ok := body["possessor"].(string)
		if !ok {
			return fmt.Errorf("missing or invalid possessor public key")
		}
		lee.Possessor = possessor
	}

	return nil
}

// handleSmartContractMessage decode sc message if at least the contract index and the message type are present
// typical message body like: `"body":{"content":"38637de88b66d44f53d1d2e0509f6d599393ad3a86ddc529ff6f2c1665bac0435153494c56455200e00f9700000000000100000000000000","scIndex":1,"scLogType":0}`
// we convert hex to binary (submitted to elastic as base64 by default)
func handleSmartContractMessage(lee *LogEventElastic, body map[string]any) error {
	var err error

	sci, ok := body["scIndex"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid emitting sc index")
	}
	lee.ContractIndex, err = toUint64(sci)
	if err != nil {
		return fmt.Errorf("converting emitting contract index: %w", err)
	}

	sct, ok := body["scLogType"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid sc log message type")
	}
	lee.ContractMessageType, err = toUint64(sct)
	if err != nil {
		return fmt.Errorf("converting contract message type: %w", err)
	}

	content, ok := body["content"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid sc message content")
	}
	lee.RawPayload, err = hex.DecodeString(content)
	if err != nil {
		return fmt.Errorf("converting hex content to raw payload: %w", err)
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
	lee.ContractIndex, err = toUint64(contractIndexBurnedFor)
	if err != nil {
		return fmt.Errorf("converting contract index burned for: %w", err)
	}

	return nil
}

func handleRaw(lee *LogEventElastic, body map[string]any) error {
	var err error

	hexValue, ok := body["hex"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid hex content")
	}
	lee.RawPayload, err = hex.DecodeString(hexValue)
	if err != nil {
		return fmt.Errorf("converting hex to raw payload: %w", err)
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

func handleOracleQueryStatusChange(lee *LogEventElastic, body map[string]any) error {
	var err error

	qe, ok := body["queryingEntity"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid querying entity")
	}
	lee.QueryingEntity = qe

	qi, ok := body["queryId"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid query id")
	}
	lee.QueryId, err = toUint64(qi)
	if err != nil {
		return fmt.Errorf("converting query id: %w", err)
	}

	ii, ok := body["interfaceIndex"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid interface index")
	}
	lee.InterfaceIndex, err = toUint64(ii)
	if err != nil {
		return fmt.Errorf("converting interface index: %w", err)
	}

	qt, ok := body["type"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid query type")
	}
	lee.QueryType, err = toPositiveInt16(qt)
	if err != nil {
		return fmt.Errorf("converting query type: %w", err)
	}

	qs, ok := body["status"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid query status")
	}
	qsv, err := getOracleQueryStatusValue(qs)
	if err != nil {
		return fmt.Errorf("invalid query status: %w", err)
	}
	lee.QueryStatus = &qsv

	return nil
}

func handleOracleSubscriberLogMessage(lee *LogEventElastic, body map[string]any) error {
	var err error

	sid, ok := body["subscriptionId"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid subscription id")
	}
	lee.SubscriptionId, err = toUint32(sid)
	if err != nil {
		return fmt.Errorf("converting subscription id: %w", err)
	}

	ii, ok := body["interfaceIndex"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid interface index")
	}
	lee.InterfaceIndex, err = toUint64(ii)
	if err != nil {
		return fmt.Errorf("converting interface index: %w", err)
	}

	ci, ok := body["contractIndex"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid contract index")
	}
	lee.ContractIndex, err = toUint64(ci)
	if err != nil {
		return fmt.Errorf("converting contract index: %w", err)
	}

	pim, ok := body["periodInMilliseconds"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid period in milliseconds")
	}
	lee.PeriodMillis, err = toUint64(pim)
	if err != nil {
		return fmt.Errorf("converting period in milliseconds: %w", err)
	}

	fqdt, ok := body["firstQueryDateAndTime"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid first query date and time")
	}
	fqdtv, err := stringToDateAndTime(fqdt)
	if err != nil {
		return fmt.Errorf("converting first query date and time: %w", err)
	}
	lee.FirstQueryTimestamp = &fqdtv

	return nil
}

func handleCustomMessage(lee *LogEventElastic, body map[string]any) error {
	var err error

	msg, ok := body["customMessage"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid custom message")
	}
	customMessageUint, err := strconv.ParseUint(msg, 10, 64)
	lee.CustomMessage = &customMessageUint
	if err != nil {
		return fmt.Errorf("converting custom message [%s]: %w", msg, err)
	}

	return nil
}

func getOracleQueryStatusValue(status string) (int16, error) {
	switch status {
	case "pending":
		return 1, nil
	case "committed":
		return 2, nil
	case "success":
		return 3, nil
	case "timeout":
		return 4, nil
	case "unresolvable":
		return 5, nil
	default:
		return 0, fmt.Errorf("unexpected oracle query status value: %s", status)
	}
}

func stringToDateAndTime(s string) (uint64, error) {
	var year, month, day, hour, minute, second, millisecond, microsecond int
	_, err := fmt.Sscanf(s, "%d-%02d-%02d %02d:%02d:%02d.%03d'%03d",
		&year, &month, &day, &hour, &minute, &second, &millisecond, &microsecond)
	if err != nil {
		return 0, fmt.Errorf("parsing date and time format: %w", err)
	}

	t := time.Date(year, time.Month(month), day, hour, minute, second,
		(millisecond*1_000+microsecond)*1_000, // nanoseconds
		time.UTC)

	return uint64(t.UnixMilli()), nil
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
	if num != float64(converted) {
		return nil, fmt.Errorf("cannot convert to int64: %f", num)
	}
	return &converted, nil
}

func toUint32(num float64) (*uint32, error) {
	converted := uint32(num)
	if num < 0 || num != float64(converted) {
		return nil, fmt.Errorf("cannot convert to uint32: %f", num)
	}
	return &converted, nil
}

func toPositiveInt16(num float64) (*int16, error) {
	converted := int16(num)
	if num < 0 || num != float64(converted) {
		return nil, fmt.Errorf("cannot convert to int16: %f", num)
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
