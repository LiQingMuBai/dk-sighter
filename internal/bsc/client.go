package bsc

import (
	"bytes"
	"context"
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

	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const (
	weiPrecision  = 1_000_000_000_000_000_000
	transferTopic = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	// QuickNode limits are enforced per account/provider. Tron and BSC use
	// separate accounts here, so each client can maintain its own ceiling.
	defaultMinRequestInterval = 21 * time.Millisecond
)

type Client struct {
	httpURL      string
	wssURL       string
	usdtContract string
	httpClient   *http.Client
	rpcID        atomic.Int64
	rateMu       sync.Mutex
	nextRequest  time.Time
	minInterval  time.Duration
}

type Block struct {
	Number       uint64
	Timestamp    uint64
	Transactions []Transaction
}

type Transaction struct {
	Hash  string
	From  string
	To    string
	Value *big.Int
}

func NewClient(httpURL, wssURL, usdtContract string) *Client {
	return &Client{
		httpURL:      strings.TrimSpace(httpURL),
		wssURL:       strings.TrimSpace(wssURL),
		usdtContract: normalizeHexAddress(usdtContract),
		httpClient: &http.Client{
			Timeout: 20 * time.Second,
		},
		minInterval: defaultMinRequestInterval,
	}
}

func (c *Client) SetMinRequestInterval(interval time.Duration) {
	if c == nil || interval <= 0 {
		return
	}
	c.rateMu.Lock()
	c.minInterval = interval
	c.rateMu.Unlock()
}

func (c *Client) USDTContract() string {
	return c.usdtContract
}

func (c *Client) BlockNumber(ctx context.Context) (uint64, error) {
	var out string
	if err := c.call(ctx, "eth_blockNumber", []any{}, &out); err != nil {
		return 0, err
	}
	return parseHexUint64(out)
}

func (c *Client) GetBlockByNumber(ctx context.Context, blockNumber uint64) (*Block, error) {
	var resp struct {
		Number       string `json:"number"`
		Timestamp    string `json:"timestamp"`
		Transactions []struct {
			Hash  string `json:"hash"`
			From  string `json:"from"`
			To    string `json:"to"`
			Value string `json:"value"`
		} `json:"transactions"`
	}

	if err := c.call(ctx, "eth_getBlockByNumber", []any{fmt.Sprintf("0x%x", blockNumber), true}, &resp); err != nil {
		return nil, err
	}

	num, err := parseHexUint64(resp.Number)
	if err != nil {
		return nil, err
	}
	timestamp, err := parseHexUint64(resp.Timestamp)
	if err != nil {
		return nil, err
	}

	txs := make([]Transaction, 0, len(resp.Transactions))
	for _, item := range resp.Transactions {
		value, err := parseHexBigInt(item.Value)
		if err != nil {
			return nil, err
		}
		txs = append(txs, Transaction{
			Hash:  item.Hash,
			From:  normalizeHexAddress(item.From),
			To:    normalizeHexAddress(item.To),
			Value: value,
		})
	}

	return &Block{
		Number:       num,
		Timestamp:    timestamp,
		Transactions: txs,
	}, nil
}

type ERC20Transfer struct {
	BlockNumber uint64
	TxHash      string
	From        string
	To          string
	Value       *big.Int
	LogIndex    uint64
}

func (c *Client) GetUSDTTransfersByBlock(ctx context.Context, blockNumber uint64) ([]ERC20Transfer, error) {
	return c.GetUSDTTransfersByRange(ctx, blockNumber, blockNumber)
}

func (c *Client) GetUSDTTransfersByRange(ctx context.Context, fromBlock, toBlock uint64) ([]ERC20Transfer, error) {
	if c.usdtContract == "" {
		return []ERC20Transfer{}, nil
	}
	if toBlock < fromBlock {
		return []ERC20Transfer{}, nil
	}

	var resp []struct {
		BlockNumber     string   `json:"blockNumber"`
		TransactionHash string   `json:"transactionHash"`
		LogIndex        string   `json:"logIndex"`
		Topics          []string `json:"topics"`
		Data            string   `json:"data"`
	}

	filter := map[string]any{
		"fromBlock": fmt.Sprintf("0x%x", fromBlock),
		"toBlock":   fmt.Sprintf("0x%x", toBlock),
		"address":   c.usdtContract,
		"topics":    []any{"0x" + transferTopic},
	}

	if err := c.call(ctx, "eth_getLogs", []any{filter}, &resp); err != nil {
		return nil, err
	}

	transfers := make([]ERC20Transfer, 0, len(resp))
	for _, logItem := range resp {
		if len(logItem.Topics) < 3 {
			continue
		}
		from := parseTopicAddress(logItem.Topics[1])
		to := parseTopicAddress(logItem.Topics[2])

		value, err := parseHexBigInt(logItem.Data)
		if err != nil {
			return nil, err
		}
		blockNumber, err := parseHexUint64(logItem.BlockNumber)
		if err != nil {
			return nil, err
		}
		logIndex, err := parseHexUint64(logItem.LogIndex)
		if err != nil {
			return nil, err
		}

		transfers = append(transfers, ERC20Transfer{
			BlockNumber: blockNumber,
			TxHash:      strings.ToLower(strings.TrimSpace(logItem.TransactionHash)),
			From:        from,
			To:          to,
			Value:       value,
			LogIndex:    logIndex,
		})
	}
	return transfers, nil
}

func (c *Client) GetUSDTTransferAddressesByBlock(ctx context.Context, blockNumber uint64) (map[string]struct{}, error) {
	transfers, err := c.GetUSDTTransfersByBlock(ctx, blockNumber)
	if err != nil {
		return nil, err
	}
	result := make(map[string]struct{})
	for _, transfer := range transfers {
		if transfer.From != "" {
			result[transfer.From] = struct{}{}
		}
		if transfer.To != "" {
			result[transfer.To] = struct{}{}
		}
	}
	return result, nil
}

func (c *Client) GetBNBBalance(ctx context.Context, address string) (decimal.Decimal, error) {
	address = normalizeHexAddress(address)
	if address == "" {
		return decimal.Zero, fmt.Errorf("empty address")
	}

	var out string
	if err := c.call(ctx, "eth_getBalance", []any{address, "latest"}, &out); err != nil {
		return decimal.Zero, err
	}

	value, err := parseHexBigInt(out)
	if err != nil {
		return decimal.Zero, err
	}
	return decimal.NewFromBigInt(value, 0).Div(decimal.NewFromInt(weiPrecision)), nil
}

func (c *Client) GetUSDTBalance(ctx context.Context, address string) (decimal.Decimal, error) {
	address = normalizeHexAddress(address)
	if address == "" {
		return decimal.Zero, fmt.Errorf("empty address")
	}
	if c.usdtContract == "" {
		return decimal.Zero, fmt.Errorf("usdt contract not configured")
	}

	data, err := buildERC20BalanceOfData(address)
	if err != nil {
		return decimal.Zero, err
	}

	callObj := map[string]any{
		"to":   c.usdtContract,
		"data": data,
	}

	var out string
	if err := c.call(ctx, "eth_call", []any{callObj, "latest"}, &out); err != nil {
		return decimal.Zero, err
	}

	value, err := parseHexBigInt(out)
	if err != nil {
		return decimal.Zero, err
	}

	return decimal.NewFromBigInt(value, 0).Div(decimal.NewFromInt(weiPrecision)), nil
}

func (c *Client) GasPrice(ctx context.Context) (*big.Int, error) {
	var out string
	if err := c.call(ctx, "eth_gasPrice", []any{}, &out); err != nil {
		return nil, err
	}
	return parseHexBigInt(out)
}

func (c *Client) ChainID(ctx context.Context) (*big.Int, error) {
	var out string
	if err := c.call(ctx, "eth_chainId", []any{}, &out); err != nil {
		return nil, err
	}
	return parseHexBigInt(out)
}

func (c *Client) PendingNonceAt(ctx context.Context, address string) (uint64, error) {
	address = normalizeHexAddress(address)
	if address == "" {
		return 0, fmt.Errorf("empty address")
	}

	var out string
	if err := c.call(ctx, "eth_getTransactionCount", []any{address, "pending"}, &out); err != nil {
		return 0, err
	}
	return parseHexUint64(out)
}

func (c *Client) EstimateGas(ctx context.Context, callObj map[string]any) (uint64, error) {
	var out string
	if err := c.call(ctx, "eth_estimateGas", []any{callObj}, &out); err != nil {
		return 0, err
	}
	return parseHexUint64(out)
}

func (c *Client) SendRawTransaction(ctx context.Context, rawHex string) (string, error) {
	rawHex = strings.TrimSpace(rawHex)
	if rawHex == "" {
		return "", fmt.Errorf("empty raw transaction")
	}
	if !strings.HasPrefix(rawHex, "0x") {
		rawHex = "0x" + rawHex
	}

	var out string
	if err := c.call(ctx, "eth_sendRawTransaction", []any{rawHex}, &out); err != nil {
		return "", err
	}
	return strings.ToLower(strings.TrimSpace(out)), nil
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

func (c *Client) call(ctx context.Context, method string, params []any, out any) error {
	if err := c.waitTurn(ctx); err != nil {
		return err
	}

	reqBody, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      c.rpcID.Add(1),
		"method":  method,
		"params":  params,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.httpURL, bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("http %d: %s", resp.StatusCode, string(body))
	}

	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("decode rpc response: %w; body=%s", err, string(body))
	}
	if envelope.Error != nil {
		message := strings.TrimSpace(envelope.Error.Message)
		if message == "" {
			message = "unknown rpc error"
		}
		return fmt.Errorf("rpc error %d: %s", envelope.Error.Code, message)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(envelope.Result, out); err != nil {
		return fmt.Errorf("decode rpc result: %w; body=%s", err, string(body))
	}
	return nil
}

func (c *Client) waitTurn(ctx context.Context) error {
	c.rateMu.Lock()
	now := time.Now()
	waitUntil := now
	if c.nextRequest.After(waitUntil) {
		waitUntil = c.nextRequest
	}
	interval := c.minInterval
	if interval <= 0 {
		interval = defaultMinRequestInterval
	}
	c.nextRequest = waitUntil.Add(interval)
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

func parseHexUint64(input string) (uint64, error) {
	clean := strings.TrimSpace(strings.TrimPrefix(input, "0x"))
	if clean == "" {
		return 0, nil
	}
	value := new(big.Int)
	if _, ok := value.SetString(clean, 16); !ok {
		return 0, fmt.Errorf("invalid hex uint64: %s", input)
	}
	return value.Uint64(), nil
}

func parseHexBigInt(input string) (*big.Int, error) {
	clean := strings.TrimSpace(strings.TrimPrefix(input, "0x"))
	if clean == "" {
		return big.NewInt(0), nil
	}
	value := new(big.Int)
	if _, ok := value.SetString(clean, 16); !ok {
		return nil, fmt.Errorf("invalid hex number: %s", input)
	}
	return value, nil
}

func normalizeHexAddress(input string) string {
	trimmed := strings.ToLower(strings.TrimSpace(input))
	if trimmed == "" || trimmed == "0x" {
		return ""
	}
	if strings.HasPrefix(trimmed, "0x") {
		if len(trimmed) == 42 {
			return trimmed
		}
		return "0x" + strings.TrimPrefix(trimmed, "0x")
	}
	return "0x" + trimmed
}

func parseTopicAddress(topic string) string {
	clean := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(topic)), "0x")
	if len(clean) >= 40 {
		clean = clean[len(clean)-40:]
	}
	return normalizeHexAddress(clean)
}

func buildERC20BalanceOfData(address string) (string, error) {
	address = normalizeHexAddress(address)
	if address == "" {
		return "", fmt.Errorf("empty address")
	}
	raw := strings.TrimPrefix(address, "0x")
	decoded, err := hex.DecodeString(raw)
	if err != nil {
		return "", err
	}
	if len(decoded) != 20 {
		return "", fmt.Errorf("invalid address length")
	}

	padded := make([]byte, 32)
	copy(padded[12:], decoded)
	return "0x70a08231" + hex.EncodeToString(padded), nil
}
