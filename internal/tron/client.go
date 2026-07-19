package tron

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gotronAddress "github.com/fbsobreira/gotron-sdk/pkg/address"
	gotronCore "github.com/fbsobreira/gotron-sdk/pkg/proto/core"
	"github.com/fbsobreira/gotron-sdk/pkg/standards/trc20enc"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	trxPrecision              = 1_000_000
	usdtPrecision             = 1_000_000
	transferTopic             = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	defaultMinRequestInterval = 10 * time.Millisecond
	trc20TransferMethodID     = "a9059cbb"
	trc20TransferFromMethodID = "23b872dd"
)

type Client struct {
	httpURL      string
	wssURL       string
	usdtContract string
	httpClient   *http.Client
	rpcID        atomic.Int64
	minInterval  time.Duration
	rateMu       sync.Mutex
	nextRequest  time.Time
}

type rpcErrorResponse struct {
	Code    *int   `json:"code"`
	Message string `json:"message"`
}

type Block struct {
	BlockHeader struct {
		RawData struct {
			Number    int64 `json:"number"`
			Timestamp int64 `json:"timestamp"`
		} `json:"raw_data"`
	} `json:"block_header"`
	Transactions []Transaction `json:"transactions"`
}

type Transaction struct {
	TxID    string `json:"txID"`
	RawData struct {
		Contract []struct {
			Type      string `json:"type"`
			Parameter struct {
				Value struct {
					Amount          int64  `json:"amount"`
					OwnerAddress    string `json:"owner_address"`
					ToAddress       string `json:"to_address"`
					ContractAddress string `json:"contract_address"`
					Data            string `json:"data"`
				} `json:"value"`
			} `json:"parameter"`
		} `json:"contract"`
	} `json:"raw_data"`
}

type TransactionInfo struct {
	ID      string `json:"id"`
	BlockNo int64  `json:"blockNumber"`
	Log     []Log  `json:"log"`
}

type Log struct {
	Address string   `json:"address"`
	Topics  []string `json:"topics"`
	Data    string   `json:"data"`
}

type Account struct {
	Address string `json:"address"`
	Balance int64  `json:"balance"`
}

type accountResource struct {
	EnergyLimit int64 `json:"EnergyLimit"`
	EnergyUsed  int64 `json:"EnergyUsed"`
}

type constantContractResp struct {
	ConstantResult []string `json:"constant_result"`
	Result         struct {
		Result bool   `json:"result"`
		Code   string `json:"code"`
	} `json:"result"`
}

type triggerSmartContractResp struct {
	Transaction json.RawMessage `json:"transaction"`
	Result      struct {
		Result  bool   `json:"result"`
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"result"`
}

type broadcastTransactionResp struct {
	Result  bool   `json:"result"`
	TxID    string `json:"txid"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Error   string `json:"Error"`
}

type tronTransactionEnvelope struct {
	Visible    bool            `json:"visible"`
	TxID       string          `json:"txID,omitempty"`
	RawData    json.RawMessage `json:"raw_data"`
	RawDataHex string          `json:"raw_data_hex,omitempty"`
	Signature  []string        `json:"signature,omitempty"`
}

type tronTransferRawDataJSON struct {
	Contract []struct {
		Type      string `json:"type"`
		Parameter struct {
			TypeURL string `json:"type_url"`
			Value   struct {
				Amount       int64  `json:"amount"`
				OwnerAddress string `json:"owner_address"`
				ToAddress    string `json:"to_address"`
			} `json:"value"`
		} `json:"parameter"`
	} `json:"contract"`
	RefBlockBytes string `json:"ref_block_bytes"`
	RefBlockHash  string `json:"ref_block_hash"`
	Expiration    int64  `json:"expiration"`
	Timestamp     int64  `json:"timestamp"`
	FeeLimit      int64  `json:"fee_limit,omitempty"`
}

func NewClient(httpURL, wssURL, usdtContract string, minRequestInterval time.Duration) *Client {
	if minRequestInterval <= 0 {
		minRequestInterval = defaultMinRequestInterval
	}
	return &Client{
		httpURL:      strings.TrimRight(httpURL, "/"),
		wssURL:       strings.TrimRight(wssURL, "/"),
		usdtContract: normalizeContractAddress(usdtContract),
		httpClient: &http.Client{
			Timeout: 20 * time.Second,
		},
		minInterval: minRequestInterval,
	}
}

func (c *Client) GetSolidBlockNumber(ctx context.Context) (int64, error) {
	var block Block
	if err := c.post(ctx, "/walletsolidity/getnowblock", map[string]any{}, &block); err != nil {
		return 0, err
	}
	return block.BlockHeader.RawData.Number, nil
}

func (c *Client) GetHeadBlockNumber(ctx context.Context) (int64, error) {
	var block Block
	if err := c.post(ctx, "/wallet/getnowblock", map[string]any{}, &block); err != nil {
		return 0, err
	}
	return block.BlockHeader.RawData.Number, nil
}

func (c *Client) GetBlockByNum(ctx context.Context, blockNum int64) (*Block, error) {
	var block Block
	if err := c.post(ctx, "/wallet/getblockbynum", map[string]any{"num": blockNum}, &block); err != nil {
		return nil, err
	}
	return &block, nil
}

func (c *Client) GetBlockByNumWithSource(ctx context.Context, blockNum int64, source string) (*Block, error) {
	var (
		block Block
		path  = "/wallet/getblockbynum"
		label = "head"
	)
	if strings.EqualFold(strings.TrimSpace(source), "solid") {
		path = "/walletsolidity/getblockbynum"
		label = "solid"
	}
	if err := c.post(ctx, path, map[string]any{"num": blockNum}, &block); err != nil {
		return nil, fmt.Errorf("get %s block by num %d: %w", label, blockNum, err)
	}
	return &block, nil
}

func (c *Client) GetTransactionInfoByID(ctx context.Context, txID string) (*TransactionInfo, error) {
	info, err := c.getTransactionInfoByIDHead(ctx, txID)
	if err == nil && !isEmptyTransactionInfo(info) {
		return info, nil
	}

	solidityInfo, solidityErr := c.getTransactionInfoByIDSolidity(ctx, txID)
	if solidityErr == nil && !isEmptyTransactionInfo(solidityInfo) {
		return solidityInfo, nil
	}

	if err == nil && isEmptyTransactionInfo(info) {
		if solidityErr != nil {
			return nil, fmt.Errorf("head tx info empty and solidity lookup failed: %w", solidityErr)
		}
		return info, nil
	}
	if err != nil && solidityErr == nil {
		return solidityInfo, nil
	}
	if err != nil && solidityErr != nil {
		return nil, fmt.Errorf("get tx info by id failed: head=%v solidity=%w", err, solidityErr)
	}

	return solidityInfo, nil
}

func (c *Client) GetTransactionInfoByIDWithSource(ctx context.Context, txID string, source string) (*TransactionInfo, error) {
	if strings.EqualFold(strings.TrimSpace(source), "solid") {
		info, err := c.getTransactionInfoByIDSolidity(ctx, txID)
		if err != nil {
			fallbackInfo, fallbackErr := c.getTransactionInfoByIDHead(ctx, txID)
			if fallbackErr != nil {
				return nil, fmt.Errorf("get solid tx info %s: %w; fallback get head tx info: %v", txID, err, fallbackErr)
			}
			return fallbackInfo, nil
		}
		return info, nil
	}

	info, err := c.getTransactionInfoByIDHead(ctx, txID)
	if err != nil {
		return nil, fmt.Errorf("get head tx info %s: %w", txID, err)
	}
	return info, nil
}

func (c *Client) getTransactionInfoByIDHead(ctx context.Context, txID string) (*TransactionInfo, error) {
	var info TransactionInfo
	if err := c.post(ctx, "/wallet/gettransactioninfobyid", map[string]any{"value": txID}, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

func (c *Client) getTransactionInfoByIDSolidity(ctx context.Context, txID string) (*TransactionInfo, error) {
	var info TransactionInfo
	if err := c.post(ctx, "/walletsolidity/gettransactioninfobyid", map[string]any{"value": txID}, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

func isEmptyTransactionInfo(info *TransactionInfo) bool {
	if info == nil {
		return true
	}
	return strings.TrimSpace(info.ID) == "" && info.BlockNo == 0 && len(info.Log) == 0
}

func (c *Client) GetAccountTRXBalance(ctx context.Context, addressHex string) (decimal.Decimal, error) {
	var account Account
	if err := c.post(ctx, "/walletsolidity/getaccount", map[string]any{
		"address": NormalizeHexAddress(addressHex),
		"visible": false,
	}, &account); err != nil {
		return decimal.Zero, err
	}
	return decimal.NewFromInt(account.Balance).Div(decimal.NewFromInt(trxPrecision)), nil
}

func (c *Client) GetAccountStateHead(ctx context.Context, addressHex string) (bool, decimal.Decimal, error) {
	var account Account
	if err := c.post(ctx, "/wallet/getaccount", map[string]any{
		"address": NormalizeHexAddress(addressHex),
		"visible": false,
	}, &account); err != nil {
		return false, decimal.Zero, err
	}
	active := strings.TrimSpace(account.Address) != ""
	balance := decimal.NewFromInt(account.Balance).Div(decimal.NewFromInt(trxPrecision))
	return active, balance, nil
}

func (c *Client) GetAccountState(ctx context.Context, addressHex string) (bool, decimal.Decimal, error) {
	var account Account
	if err := c.post(ctx, "/walletsolidity/getaccount", map[string]any{
		"address": NormalizeHexAddress(addressHex),
		"visible": false,
	}, &account); err != nil {
		return false, decimal.Zero, err
	}
	active := strings.TrimSpace(account.Address) != ""
	balance := decimal.NewFromInt(account.Balance).Div(decimal.NewFromInt(trxPrecision))
	return active, balance, nil
}

func (c *Client) GetAvailableEnergy(ctx context.Context, addressHex string) (int64, error) {
	var resource accountResource
	if err := c.post(ctx, "/wallet/getaccountresource", map[string]any{
		"address": NormalizeHexAddress(addressHex),
		"visible": false,
	}, &resource); err != nil {
		return 0, err
	}
	available := resource.EnergyLimit - resource.EnergyUsed
	if available < 0 {
		return 0, nil
	}
	return available, nil
}

func (c *Client) GetUSDTBalance(ctx context.Context, addressHex string) (decimal.Decimal, error) {
	addressHex = NormalizeHexAddress(addressHex)
	owner, err := hex.DecodeString(addressHex)
	if err != nil {
		return decimal.Zero, fmt.Errorf("decode owner hex: %w", err)
	}

	addressParam := strings.Repeat("0", 24) + strings.ToLower(hex.EncodeToString(owner[1:]))

	var resp constantContractResp
	err = c.post(ctx, "/wallet/triggerconstantcontract", map[string]any{
		"owner_address":     addressHex,
		"contract_address":  c.usdtContract,
		"function_selector": "balanceOf(address)",
		"parameter":         addressParam,
		"visible":           false,
	}, &resp)
	if err != nil {
		return decimal.Zero, err
	}
	if !resp.Result.Result || len(resp.ConstantResult) == 0 {
		return decimal.Zero, fmt.Errorf("trigger constant contract failed: %s", resp.Result.Code)
	}

	value, err := decimal.NewFromString(parseHexNumber(resp.ConstantResult[0]))
	if err != nil {
		return decimal.Zero, fmt.Errorf("parse usdt balance: %w", err)
	}
	return value.Div(decimal.NewFromInt(usdtPrecision)), nil
}

func (c *Client) SubscribeNewHeads(ctx context.Context, onMessage func()) error {
	if c.wssURL == "" {
		return nil
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.wssURL, nil)
	if err != nil {
		return fmt.Errorf("dial websocket: %w", err)
	}
	defer conn.Close()

	subscribePayload := map[string]any{
		"jsonrpc": "2.0",
		"id":      c.rpcID.Add(1),
		"method":  "eth_subscribe",
		"params":  []any{"newHeads"},
	}
	if err := conn.WriteJSON(subscribePayload); err != nil {
		return fmt.Errorf("subscribe newHeads: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read websocket: %w", err)
		}

		if bytes.Contains(message, []byte(`"eth_subscription"`)) {
			onMessage()
		}
	}
}

func (c *Client) IsUSDTTransferLog(logItem Log) bool {
	if NormalizeHexAddress(logItem.Address) != c.usdtContract {
		return false
	}
	if len(logItem.Topics) == 0 {
		return false
	}
	return strings.EqualFold(strings.TrimPrefix(logItem.Topics[0], "0x"), transferTopic)
}

// ShouldInspectUSDTTriggerTx uses the raw TriggerSmartContract input to skip
// `gettransactioninfobyid` calls for transactions that cannot produce watched
// USDT transfer records in the current direct transfer flow.
func (c *Client) ShouldInspectUSDTTriggerTx(tx Transaction, isWatchedHex func(string) bool) bool {
	if len(tx.RawData.Contract) == 0 {
		return false
	}
	contract := tx.RawData.Contract[0]
	if contract.Type != "TriggerSmartContract" {
		return false
	}

	value := contract.Parameter.Value
	if NormalizeHexAddress(value.ContractAddress) != c.usdtContract {
		return false
	}
	if isWatchedHex == nil {
		return true
	}

	methodID, ok := extractTriggerMethodID(value.Data)
	if !ok {
		// Keep malformed or unexpected calldata conservative.
		return true
	}

	switch methodID {
	case trc20TransferMethodID:
		toHex, ok := decodeTriggerAddressArg(value.Data, 0)
		if !ok {
			return true
		}
		return isWatchedHex(value.OwnerAddress) || isWatchedHex(toHex)
	case trc20TransferFromMethodID:
		fromHex, okFrom := decodeTriggerAddressArg(value.Data, 0)
		toHex, okTo := decodeTriggerAddressArg(value.Data, 1)
		if !okFrom || !okTo {
			return true
		}
		return isWatchedHex(fromHex) || isWatchedHex(toHex)
	default:
		return false
	}
}

func (c *Client) DecodeTransferLog(logItem Log) (fromHex string, toHex string, amount decimal.Decimal, err error) {
	if len(logItem.Topics) < 3 {
		return "", "", decimal.Zero, fmt.Errorf("insufficient topics")
	}

	fromHex = parseTopicAddress(logItem.Topics[1])
	toHex = parseTopicAddress(logItem.Topics[2])

	value, err := decimal.NewFromString(parseHexNumber(logItem.Data))
	if err != nil {
		return "", "", decimal.Zero, fmt.Errorf("parse log amount: %w", err)
	}

	return fromHex, toHex, value.Div(decimal.NewFromInt(usdtPrecision)), nil
}

func (c *Client) CreateUSDTTransferTransaction(ctx context.Context, ownerHex, toAddress string, amount *big.Int, feeLimit int64) ([]byte, error) {
	ownerHex = NormalizeHexAddress(ownerHex)
	if ownerHex == "" {
		return nil, fmt.Errorf("empty owner address")
	}
	if amount == nil || amount.Sign() <= 0 {
		return nil, fmt.Errorf("amount must be positive")
	}
	if feeLimit <= 0 {
		feeLimit = 30_000_000
	}

	toAddr, err := gotronAddress.Base58ToAddress(strings.TrimSpace(toAddress))
	if err != nil {
		return nil, fmt.Errorf("invalid destination address: %w", err)
	}
	dataHex, err := trc20enc.EncodeTransferCall(toAddr, amount)
	if err != nil {
		return nil, fmt.Errorf("encode transfer call: %w", err)
	}

	var resp triggerSmartContractResp
	err = c.post(ctx, "/wallet/triggersmartcontract", map[string]any{
		"owner_address":    ownerHex,
		"contract_address": c.usdtContract,
		"data":             dataHex,
		"fee_limit":        feeLimit,
		"call_value":       0,
		"visible":          false,
	}, &resp)
	if err != nil {
		return nil, err
	}
	if !resp.Result.Result {
		message := strings.TrimSpace(resp.Result.Message)
		if message == "" {
			message = strings.TrimSpace(resp.Result.Code)
		}
		if message == "" {
			message = "trigger smart contract failed"
		}
		return nil, fmt.Errorf("%s", message)
	}
	if len(resp.Transaction) == 0 {
		return nil, fmt.Errorf("empty unsigned transaction")
	}
	return resp.Transaction, nil
}

func (c *Client) CreateTRXTransferTransaction(ctx context.Context, ownerHex, toAddress string, amountSun int64) ([]byte, error) {
	ownerHex = NormalizeHexAddress(ownerHex)
	if ownerHex == "" {
		return nil, fmt.Errorf("empty owner address")
	}
	if amountSun <= 0 {
		return nil, fmt.Errorf("amount must be positive")
	}

	toHex, err := normalizeTronAddressToHex(toAddress)
	if err != nil {
		return nil, err
	}

	respBody, err := c.postRaw(ctx, "/wallet/createtransaction", map[string]any{
		"to_address":    toHex,
		"owner_address": ownerHex,
		"amount":        amountSun,
		"visible":       false,
	})
	if err != nil {
		return nil, err
	}
	if len(respBody) == 0 {
		return nil, fmt.Errorf("empty unsigned transaction")
	}
	return ensureRawDataHex(respBody)
}

func (c *Client) BroadcastTransactionJSON(ctx context.Context, txJSON []byte) (string, error) {
	if len(txJSON) == 0 {
		return "", fmt.Errorf("empty transaction json")
	}

	var payload any
	if err := json.Unmarshal(txJSON, &payload); err != nil {
		return "", fmt.Errorf("decode signed transaction json: %w", err)
	}

	respBody, err := c.postRaw(ctx, "/wallet/broadcasttransaction", payload)
	if err != nil {
		return "", err
	}

	var resp broadcastTransactionResp
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return "", fmt.Errorf("decode broadcast response: %w; body=%s", err, string(respBody))
	}
	if !resp.Result {
		code := strings.TrimSpace(resp.Code)
		message := normalizeTronReturnMessage(resp.Message)
		txID := strings.TrimSpace(resp.TxID)
		respError := strings.TrimSpace(resp.Error)
		rawResponse := compactJSONForLog(respBody)
		switch {
		case respError != "":
			return "", fmt.Errorf("broadcast transaction failed: error=%s", respError)
		case code != "" && message != "" && txID != "":
			return "", fmt.Errorf("broadcast transaction failed: code=%s message=%s txid=%s", code, message, txID)
		case code != "" && message != "":
			return "", fmt.Errorf("broadcast transaction failed: code=%s message=%s", code, message)
		case code != "" && txID != "":
			return "", fmt.Errorf("broadcast transaction failed: code=%s txid=%s", code, txID)
		case message != "" && txID != "":
			return "", fmt.Errorf("broadcast transaction failed: message=%s txid=%s", message, txID)
		case code != "":
			return "", fmt.Errorf("broadcast transaction failed: code=%s", code)
		case message != "":
			return "", fmt.Errorf("broadcast transaction failed: message=%s", message)
		case txID != "":
			return "", fmt.Errorf("broadcast transaction failed: txid=%s", txID)
		case rawResponse != "":
			return "", fmt.Errorf("broadcast transaction failed: raw_response=%s", rawResponse)
		default:
			return "", fmt.Errorf("broadcast transaction failed")
		}
	}
	return strings.TrimSpace(resp.TxID), nil
}

func normalizeTronAddressToHex(input string) (string, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return "", fmt.Errorf("empty address")
	}
	if strings.HasPrefix(trimmed, "T") {
		addr, err := gotronAddress.Base58ToAddress(trimmed)
		if err != nil {
			return "", fmt.Errorf("invalid destination address: %w", err)
		}
		return strings.ToUpper(hex.EncodeToString(addr.Bytes())), nil
	}
	return NormalizeHexAddress(trimmed), nil
}

func ensureRawDataHex(txJSON []byte) ([]byte, error) {
	if len(txJSON) == 0 {
		return nil, fmt.Errorf("empty transaction json")
	}

	var envelope tronTransactionEnvelope
	if err := json.Unmarshal(txJSON, &envelope); err != nil {
		return nil, fmt.Errorf("decode transaction json: %w", err)
	}
	if strings.TrimSpace(envelope.RawDataHex) != "" {
		return txJSON, nil
	}

	rawDataHex, txID, err := encodeTransferRawDataHex(envelope.RawData)
	if err != nil {
		return nil, err
	}
	envelope.RawDataHex = rawDataHex
	if strings.TrimSpace(envelope.TxID) == "" {
		envelope.TxID = txID
	}

	encoded, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encode transaction json: %w", err)
	}
	return encoded, nil
}

func encodeTransferRawDataHex(rawDataJSON []byte) (string, string, error) {
	if len(rawDataJSON) == 0 {
		return "", "", fmt.Errorf("missing raw_data field")
	}

	var raw tronTransferRawDataJSON
	if err := json.Unmarshal(rawDataJSON, &raw); err != nil {
		return "", "", fmt.Errorf("decode raw_data: %w", err)
	}
	if len(raw.Contract) == 0 {
		return "", "", fmt.Errorf("missing raw_data.contract")
	}

	contracts := make([]*gotronCore.Transaction_Contract, 0, len(raw.Contract))
	for i, item := range raw.Contract {
		typeURL := strings.TrimSpace(item.Parameter.TypeURL)
		contractType := strings.TrimSpace(item.Type)
		if !strings.EqualFold(contractType, "TransferContract") && !strings.HasSuffix(typeURL, ".TransferContract") {
			return "", "", fmt.Errorf("unsupported contract type without raw_data_hex: %s", contractType)
		}

		ownerAddress, err := hex.DecodeString(strings.TrimSpace(item.Parameter.Value.OwnerAddress))
		if err != nil {
			return "", "", fmt.Errorf("decode owner address for contract %d: %w", i, err)
		}
		toAddress, err := hex.DecodeString(strings.TrimSpace(item.Parameter.Value.ToAddress))
		if err != nil {
			return "", "", fmt.Errorf("decode to address for contract %d: %w", i, err)
		}

		transferAny, err := anypb.New(&gotronCore.TransferContract{
			OwnerAddress: ownerAddress,
			ToAddress:    toAddress,
			Amount:       item.Parameter.Value.Amount,
		})
		if err != nil {
			return "", "", fmt.Errorf("build transfer contract %d: %w", i, err)
		}
		contracts = append(contracts, &gotronCore.Transaction_Contract{
			Type:      gotronCore.Transaction_Contract_TransferContract,
			Parameter: transferAny,
		})
	}

	refBlockBytes, err := hex.DecodeString(strings.TrimSpace(raw.RefBlockBytes))
	if err != nil {
		return "", "", fmt.Errorf("decode ref_block_bytes: %w", err)
	}
	refBlockHash, err := hex.DecodeString(strings.TrimSpace(raw.RefBlockHash))
	if err != nil {
		return "", "", fmt.Errorf("decode ref_block_hash: %w", err)
	}

	rawProto := &gotronCore.TransactionRaw{
		RefBlockBytes: refBlockBytes,
		RefBlockHash:  refBlockHash,
		Expiration:    raw.Expiration,
		Contract:      contracts,
		Timestamp:     raw.Timestamp,
		FeeLimit:      raw.FeeLimit,
	}

	rawBytes, err := proto.Marshal(rawProto)
	if err != nil {
		return "", "", fmt.Errorf("marshal raw_data: %w", err)
	}
	txIDBytes := sha256.Sum256(rawBytes)
	return hex.EncodeToString(rawBytes), hex.EncodeToString(txIDBytes[:]), nil
}

func normalizeTronReturnMessage(message string) string {
	message = strings.TrimSpace(message)
	if message == "" {
		return ""
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(message, "0x"))
	if err != nil {
		return message
	}
	decodedText := strings.TrimSpace(string(decoded))
	if decodedText == "" {
		return message
	}
	return decodedText
}

func compactJSONForLog(body []byte) string {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return ""
	}
	var compacted bytes.Buffer
	if err := json.Compact(&compacted, []byte(trimmed)); err != nil {
		return trimmed
	}
	return compacted.String()
}

func (c *Client) post(ctx context.Context, path string, payload any, out any) error {
	respBody, err := c.postRaw(ctx, path, payload)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(respBody, out); err != nil {
		return fmt.Errorf("decode response: %w; body=%s", err, string(respBody))
	}
	return nil
}

func (c *Client) postRaw(ctx context.Context, path string, payload any) ([]byte, error) {
	if err := c.waitTurn(ctx); err != nil {
		return nil, err
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.httpURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("post %s: %w", path, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, string(respBody))
	}
	if err := detectRPCError(respBody); err != nil {
		return nil, err
	}
	return respBody, nil
}

func (c *Client) waitTurn(ctx context.Context) error {
	c.rateMu.Lock()
	now := time.Now()
	waitUntil := now
	if c.nextRequest.After(waitUntil) {
		waitUntil = c.nextRequest
	}
	c.nextRequest = waitUntil.Add(c.minInterval)
	c.rateMu.Unlock()

	if waitUntil.After(now) {
		timer := time.NewTimer(time.Until(waitUntil))
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}

func detectRPCError(respBody []byte) error {
	var rpcErr rpcErrorResponse
	if err := json.Unmarshal(respBody, &rpcErr); err != nil {
		return nil
	}
	if rpcErr.Code == nil {
		return nil
	}

	message := strings.TrimSpace(rpcErr.Message)
	if message == "" {
		message = "unknown rpc error"
	}
	return fmt.Errorf("rpc error %d: %s", *rpcErr.Code, message)
}

func parseHexNumber(input string) string {
	clean := strings.TrimPrefix(strings.TrimSpace(input), "0x")
	clean = strings.TrimLeft(clean, "0")
	if clean == "" {
		return "0"
	}

	n := decimal.Zero
	base := decimal.NewFromInt(16)
	for _, r := range clean {
		n = n.Mul(base).Add(decimal.NewFromInt(int64(hexValue(byte(r)))))
	}
	return n.String()
}

func hexValue(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	default:
		return 0
	}
}

func parseTopicAddress(topic string) string {
	clean := strings.TrimPrefix(strings.TrimSpace(topic), "0x")
	if len(clean) >= 40 {
		clean = clean[len(clean)-40:]
	}
	return NormalizeHexAddress(clean)
}

func normalizeContractAddress(input string) string {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "T") {
		if hexAddr, err := Base58ToHex(trimmed); err == nil {
			return NormalizeHexAddress(hexAddr)
		}
	}
	return NormalizeHexAddress(trimmed)
}

func extractTriggerMethodID(data string) (string, bool) {
	clean := strings.TrimPrefix(strings.TrimSpace(data), "0x")
	if len(clean) < 8 {
		return "", false
	}
	return strings.ToLower(clean[:8]), true
}

func decodeTriggerAddressArg(data string, argIndex int) (string, bool) {
	if argIndex < 0 {
		return "", false
	}
	clean := strings.TrimPrefix(strings.TrimSpace(data), "0x")
	start := 8 + argIndex*64
	end := start + 64
	if len(clean) < end {
		return "", false
	}
	word := clean[start:end]
	if len(word) != 64 {
		return "", false
	}
	return NormalizeHexAddress(word[24:]), true
}
