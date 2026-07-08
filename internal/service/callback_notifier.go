package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"tron_watcher/internal/config"
	"tron_watcher/internal/repository"
)

type CallbackNotifier struct {
	enabled    bool
	url        string
	timeout    time.Duration
	httpClient *http.Client
	logger     *log.Logger
	minAmount  decimal.Decimal

	queue chan callbackPayload

	mu       sync.Mutex
	recent   map[string]time.Time
	retain   time.Duration
	cleanN   int
	cleanCnt int
}

type callbackPayload struct {
	Chain            string `json:"chain"`
	Direction        string `json:"direction"`
	Status           string `json:"status"`
	WatchAddress     string `json:"watch_address"`
	AssetCode        string `json:"asset_code"`
	Amount           string `json:"amount"`
	FromAddress      string `json:"from_address"`
	ToAddress        string `json:"to_address"`
	BlockNumber      int64  `json:"block_number"`
	BlockTime        int64  `json:"block_time"`
	BlockTimeRFC3339 string `json:"block_time_rfc3339"`
	TxHash           string `json:"tx_hash"`
	LogIndex         int    `json:"log_index"`
	Text             string `json:"text"`
}

func NewCallbackNotifier(cfg config.CallbackConfig) *CallbackNotifier {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = 256
	}
	minAmount, err := decimal.NewFromString(strings.TrimSpace(cfg.MinAmount))
	if err != nil || minAmount.IsNegative() {
		minAmount = decimal.NewFromInt(1)
	}

	enabled := cfg.Enabled && strings.TrimSpace(cfg.URL) != ""

	return &CallbackNotifier{
		enabled: enabled,
		url:     strings.TrimSpace(cfg.URL),
		timeout: timeout,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger:    tronLogger(),
		minAmount: minAmount,
		queue:     make(chan callbackPayload, queueSize),
		recent:    make(map[string]time.Time),
		retain:    10 * time.Minute,
		cleanN:    256,
	}
}

func (n *CallbackNotifier) NotifyTransfer(ctx context.Context, chain string, direction string, record repository.TransferRecord) {
	if !n.enabled {
		return
	}
	if record.TxHash == "" || record.AssetCode == "" || record.WatchAddress == "" {
		return
	}
	if record.Amount.Cmp(n.minAmount) <= 0 {
		return
	}

	key := fmt.Sprintf("%s|%s|%s|%s|%s|%d", chain, direction, record.TxHash, record.AssetCode, record.WatchAddress, record.LogIndex)
	now := time.Now()

	n.mu.Lock()
	if last, ok := n.recent[key]; ok && now.Sub(last) < n.retain {
		n.mu.Unlock()
		return
	}
	n.recent[key] = now
	n.cleanCnt++
	needClean := n.cleanCnt >= n.cleanN
	if needClean {
		n.cleanCnt = 0
	}
	n.mu.Unlock()

	if needClean {
		n.clean(now)
	}

	payload := callbackPayload{
		Chain:            strings.ToUpper(chain),
		Direction:        strings.ToUpper(direction),
		Status:           record.Status,
		WatchAddress:     record.WatchAddress,
		AssetCode:        record.AssetCode,
		Amount:           record.Amount.String(),
		FromAddress:      record.FromAddress,
		ToAddress:        record.ToAddress,
		BlockNumber:      record.BlockNumber,
		BlockTime:        record.BlockTime,
		BlockTimeRFC3339: time.UnixMilli(record.BlockTime).In(time.FixedZone("CST", 8*3600)).Format(time.RFC3339),
		TxHash:           record.TxHash,
		LogIndex:         record.LogIndex,
		Text:             formatTransferText(chain, direction, record),
	}

	select {
	case n.queue <- payload:
	default:
		n.logger.Printf("callback queue full, drop message: chain=%s dir=%s tx=%s", chain, direction, record.TxHash)
	}
}

func (n *CallbackNotifier) Run(ctx context.Context) error {
	if !n.enabled {
		<-ctx.Done()
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case payload := <-n.queue:
			n.send(ctx, payload)
		}
	}
}

func (n *CallbackNotifier) send(ctx context.Context, payload callbackPayload) {
	data, err := json.Marshal(payload)
	if err != nil {
		n.logger.Printf("callback marshal failed: %v", err)
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.url, bytes.NewReader(data))
	if err != nil {
		n.logger.Printf("callback build request failed: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.httpClient.Do(req)
	if err != nil {
		n.logger.Printf("callback send failed: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		n.logger.Printf("callback send non-2xx: %s", resp.Status)
	}
}

func (n *CallbackNotifier) clean(now time.Time) {
	cutoff := now.Add(-n.retain)
	n.mu.Lock()
	for k, t := range n.recent {
		if t.Before(cutoff) {
			delete(n.recent, k)
		}
	}
	n.mu.Unlock()
}
