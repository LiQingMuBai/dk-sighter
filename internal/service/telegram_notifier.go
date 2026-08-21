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

type telegramInlineButton struct {
	Text     string         `json:"text"`
	URL      string         `json:"url,omitempty"`
	CopyText map[string]any `json:"copy_text,omitempty"`
}

type telegramChatMessage struct {
	chatID  string
	text    string
	buttons [][]telegramInlineButton
}

type TelegramNotifier struct {
	enabled    bool
	baseURL    string
	token      string
	chatID     string
	timeout    time.Duration
	httpClient *http.Client
	logger     *log.Logger
	minAmount  decimal.Decimal

	queue chan string
	directQueue chan telegramChatMessage

	mu       sync.Mutex
	recent   map[string]time.Time
	retain   time.Duration
	cleanN   int
	cleanCnt int
}

func NewTelegramNotifier(cfg config.TelegramConfig) *TelegramNotifier {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = 256
	}
	directQueueSize := queueSize * 4
	if directQueueSize <= 0 {
		directQueueSize = 1024
	}
	baseURL := strings.TrimSpace(cfg.APIBaseURL)
	if baseURL == "" {
		baseURL = "https://api.telegram.org"
	}

	enabled := cfg.Enabled
	if strings.TrimSpace(cfg.BotToken) == "" {
		enabled = false
	}
	minAmount, err := decimal.NewFromString(strings.TrimSpace(cfg.MinAmount))
	if err != nil || minAmount.IsNegative() {
		minAmount = decimal.NewFromInt(1)
	}

	return &TelegramNotifier{
		enabled:   enabled,
		baseURL:   strings.TrimRight(baseURL, "/"),
		token:     strings.TrimSpace(cfg.BotToken),
		chatID:    strings.TrimSpace(cfg.ChatID),
		timeout:   timeout,
		minAmount: minAmount,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: tronLogger(),
		queue:  make(chan string, queueSize),
		directQueue: make(chan telegramChatMessage, directQueueSize),
		recent: make(map[string]time.Time),
		retain: 10 * time.Minute,
		cleanN: 256,
	}
}

func (n *TelegramNotifier) NotifyTransfer(ctx context.Context, chain string, direction string, record repository.TransferRecord) {
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

	text := n.format(chain, direction, record)
	select {
	case n.queue <- text:
	default:
		n.logger.Printf("telegram queue full, drop message: chain=%s dir=%s tx=%s", chain, direction, record.TxHash)
	}
}

func (n *TelegramNotifier) SetLogger(logger *log.Logger) {
	if n == nil || logger == nil {
		return
	}
	n.logger = logger
}

func (n *TelegramNotifier) Run(ctx context.Context) error {
	if !n.enabled {
		<-ctx.Done()
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-n.queue:
			n.send(ctx, n.chatID, msg, nil)
		case direct := <-n.directQueue:
			if strings.TrimSpace(direct.chatID) == "" {
				continue
			}
			n.send(ctx, direct.chatID, direct.text, direct.buttons)
		}
	}
}

func formatTransferText(chain string, direction string, record repository.TransferRecord) string {
	t := time.UnixMilli(record.BlockTime).In(time.FixedZone("CST", 8*3600)).Format("2006-01-02 15:04:05")
	return fmt.Sprintf(
		"[%s] %s %s\naddr=%s\nasset=%s amount=%s\nfrom=%s\nto=%s\nblock=%d time=%s\nhash=%s",
		strings.ToUpper(chain),
		strings.ToUpper(direction),
		record.Status,
		record.WatchAddress,
		record.AssetCode,
		record.Amount.String(),
		record.FromAddress,
		record.ToAddress,
		record.BlockNumber,
		t,
		record.TxHash,
	)
}

func (n *TelegramNotifier) format(chain string, direction string, record repository.TransferRecord) string {
	return formatTransferText(chain, direction, record)
}

func (n *TelegramNotifier) send(ctx context.Context, chatID string, text string, buttons [][]telegramInlineButton) {
	reqBody := map[string]any{
		"chat_id":    chatID,
		"text":       text,
		"parse_mode": "HTML",
	}
	if len(buttons) > 0 {
		reqBody["reply_markup"] = map[string]any{"inline_keyboard": buttons}
	}
	data, _ := json.Marshal(reqBody)

	url := fmt.Sprintf("%s/bot%s/sendMessage", n.baseURL, n.token)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		n.logger.Printf("telegram send build request failed: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.httpClient.Do(req)
	if err != nil {
		n.logger.Printf("telegram send failed: chat_id=%s err=%v", chatID, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		n.logger.Printf("telegram send non-2xx: chat_id=%s status=%s", chatID, resp.Status)
	}
}

func (n *TelegramNotifier) SendToChatID(ctx context.Context, chatID string, text string, buttons ...[][]telegramInlineButton) {
	if !n.enabled {
		return
	}
	chatID = strings.TrimSpace(chatID)
	if chatID == "" || strings.TrimSpace(text) == "" {
		return
	}
	msg := telegramChatMessage{
		chatID: chatID,
		text:   text,
	}
	if len(buttons) > 0 && buttons[0] != nil {
		msg.buttons = buttons[0]
	}
	select {
	case n.directQueue <- msg:
	default:
		n.logger.Printf("telegram direct queue full, drop message: chat_id=%s", chatID)
	}
}

func (n *TelegramNotifier) clean(now time.Time) {
	cutoff := now.Add(-n.retain)
	n.mu.Lock()
	for k, t := range n.recent {
		if t.Before(cutoff) {
			delete(n.recent, k)
		}
	}
	n.mu.Unlock()
}
