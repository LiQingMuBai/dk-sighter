package service

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"

	"tron_watcher/internal/repository"
	"tron_watcher/internal/tron"
)

const tronBalanceWorkers = 4
const tronUSDTRepairTimeout = 30 * time.Second
const tronUSDTRepairLookback = 5 * time.Minute
const tronImmediateBalanceRefreshDelay = 30 * time.Second
const tronImmediateBalanceRefreshTimeout = 12 * time.Second

var usdtTransferRepairThreshold = decimal.NewFromInt(1)

type tronBalanceTask struct {
	addressBase58 string
	addressHex    string
	asset         string
	preferHead    bool
}

type tronImmediateBalanceRequest struct {
	addressBase58 string
	addressHex    string
	asset         string
	blockNumber   int64
	triggerAsset  string
	direction     string
	txID          string
}

type BalanceService struct {
	tronClient *tron.Client
	repo       *repository.DB
	cache      *AddressCache
	logger     *log.Logger

	mu                  sync.Mutex
	dirty               map[string]map[string]struct{}
	usdtRepairing       map[string]struct{}
	immediatePending    map[string]map[string]tronImmediateBalanceRequest
	immediateRefreshing map[string]map[string]struct{}
}

func NewBalanceService(tronClient *tron.Client, repo *repository.DB, cache *AddressCache) *BalanceService {
	return &BalanceService{
		tronClient:          tronClient,
		repo:                repo,
		cache:               cache,
		logger:              tronLogger(),
		dirty:               make(map[string]map[string]struct{}),
		usdtRepairing:       make(map[string]struct{}),
		immediatePending:    make(map[string]map[string]tronImmediateBalanceRequest),
		immediateRefreshing: make(map[string]map[string]struct{}),
	}
}

func (s *BalanceService) Mark(addressBase58, asset string) {
	if addressBase58 == "" || asset == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.dirty[addressBase58]; !ok {
		s.dirty[addressBase58] = make(map[string]struct{})
	}
	s.dirty[addressBase58][asset] = struct{}{}
}

func (s *BalanceService) TriggerImmediateRefresh(addressBase58 string, assets []string, txID string, blockNumber int64, triggerAsset, direction string) {
	if s == nil || s.tronClient == nil || s.repo == nil {
		return
	}
	addressBase58 = strings.TrimSpace(addressBase58)
	if addressBase58 == "" {
		return
	}

	addressHex, err := tron.Base58ToHex(addressBase58)
	if err != nil {
		s.logger.Printf("convert address to hex failed for immediate refresh: %s err=%v", addressBase58, err)
		return
	}

	normalizedAssets := make([]string, 0, len(assets))
	seenAssets := make(map[string]struct{}, len(assets))
	for _, asset := range assets {
		asset = strings.ToUpper(strings.TrimSpace(asset))
		if asset == "" {
			continue
		}
		if _, exists := seenAssets[asset]; exists {
			continue
		}
		seenAssets[asset] = struct{}{}
		normalizedAssets = append(normalizedAssets, asset)
	}
	if len(normalizedAssets) == 0 {
		return
	}

	for _, asset := range normalizedAssets {
		req := tronImmediateBalanceRequest{
			addressBase58: addressBase58,
			addressHex:    addressHex,
			asset:         asset,
			blockNumber:   blockNumber,
			triggerAsset:  strings.ToUpper(strings.TrimSpace(triggerAsset)),
			direction:     strings.ToUpper(strings.TrimSpace(direction)),
			txID:          strings.TrimSpace(txID),
		}
		s.enqueueImmediateRefresh(req)
	}
}

func (s *BalanceService) enqueueImmediateRefresh(req tronImmediateBalanceRequest) {
	s.mu.Lock()
	if _, ok := s.immediatePending[req.addressBase58]; !ok {
		s.immediatePending[req.addressBase58] = make(map[string]tronImmediateBalanceRequest)
	}
	s.immediatePending[req.addressBase58][req.asset] = req
	// #region debug-point A:immediate-refresh-enqueue
	balanceDebugReport("A", "balance.immediate.enqueue", map[string]any{
		"address":       req.addressBase58,
		"asset":         req.asset,
		"block":         req.blockNumber,
		"trigger_asset": req.triggerAsset,
		"direction":     req.direction,
		"tx":            req.txID,
	})
	// #endregion
	if _, ok := s.immediateRefreshing[req.addressBase58]; ok {
		if _, running := s.immediateRefreshing[req.addressBase58][req.asset]; running {
			// #region debug-point A:immediate-refresh-deduped
			balanceDebugReport("A", "balance.immediate.deduped", map[string]any{
				"address": req.addressBase58,
				"asset":   req.asset,
				"block":   req.blockNumber,
				"tx":      req.txID,
			})
			// #endregion
			s.mu.Unlock()
			return
		}
	} else {
		s.immediateRefreshing[req.addressBase58] = make(map[string]struct{})
	}
	s.immediateRefreshing[req.addressBase58][req.asset] = struct{}{}
	s.mu.Unlock()

	go s.runImmediateRefreshLoop(req.addressBase58, req.asset)
}

func (s *BalanceService) runImmediateRefreshLoop(addressBase58, asset string) {
	for {
		req, ok := s.takeImmediateRefreshRequest(addressBase58, asset)
		if !ok {
			return
		}

		// #region debug-point A:immediate-refresh-scheduled
		balanceDebugReport("A", "balance.immediate.scheduled", map[string]any{
			"address":       req.addressBase58,
			"asset":         req.asset,
			"block":         req.blockNumber,
			"trigger_asset": req.triggerAsset,
			"direction":     req.direction,
			"tx":            req.txID,
			"delay_ms":      tronImmediateBalanceRefreshDelay.Milliseconds(),
		})
		// #endregion
		s.logger.Printf("transfer matched -> delayed balance refresh scheduled: address=%s trigger_asset=%s refresh_asset=%s direction=%s tx=%s block=%d delay=%s source=onchain",
			req.addressBase58, req.triggerAsset, req.asset, req.direction, req.txID, req.blockNumber, tronImmediateBalanceRefreshDelay)

		timer := time.NewTimer(tronImmediateBalanceRefreshDelay)
		<-timer.C

		req = s.takeLatestImmediateRefreshRequest(req.addressBase58, req.asset, req)

		// #region debug-point A:immediate-refresh-start
		balanceDebugReport("A", "balance.immediate.start", map[string]any{
			"address":       req.addressBase58,
			"asset":         req.asset,
			"block":         req.blockNumber,
			"trigger_asset": req.triggerAsset,
			"direction":     req.direction,
			"tx":            req.txID,
			"delay_ms":      tronImmediateBalanceRefreshDelay.Milliseconds(),
		})
		// #endregion
		s.logger.Printf("transfer matched -> delayed balance refresh started: address=%s trigger_asset=%s refresh_asset=%s direction=%s tx=%s block=%d delay=%s source=onchain",
			req.addressBase58, req.triggerAsset, req.asset, req.direction, req.txID, req.blockNumber, tronImmediateBalanceRefreshDelay)

		refreshCtx, cancel := context.WithTimeout(context.Background(), tronImmediateBalanceRefreshTimeout)
		s.refreshBalance(refreshCtx, tronBalanceTask{
			addressBase58: req.addressBase58,
			addressHex:    req.addressHex,
			asset:         req.asset,
			preferHead:    req.asset == "TRX",
		}, req.blockNumber)
		cancel()
	}
}

func (s *BalanceService) takeLatestImmediateRefreshRequest(addressBase58, asset string, fallback tronImmediateBalanceRequest) tronImmediateBalanceRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	reqsByAddress, ok := s.immediatePending[addressBase58]
	if !ok {
		return fallback
	}
	req, ok := reqsByAddress[asset]
	if !ok {
		return fallback
	}
	delete(reqsByAddress, asset)
	if len(reqsByAddress) == 0 {
		delete(s.immediatePending, addressBase58)
	}
	return req
}

func (s *BalanceService) takeImmediateRefreshRequest(addressBase58, asset string) (tronImmediateBalanceRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reqsByAddress, ok := s.immediatePending[addressBase58]
	if !ok {
		s.finishImmediateRefreshLocked(addressBase58, asset)
		return tronImmediateBalanceRequest{}, false
	}
	req, ok := reqsByAddress[asset]
	if !ok {
		s.finishImmediateRefreshLocked(addressBase58, asset)
		return tronImmediateBalanceRequest{}, false
	}
	delete(reqsByAddress, asset)
	if len(reqsByAddress) == 0 {
		delete(s.immediatePending, addressBase58)
	}
	return req, true
}

func (s *BalanceService) finishImmediateRefreshLocked(addressBase58, asset string) {
	refreshingByAddress, ok := s.immediateRefreshing[addressBase58]
	if !ok {
		return
	}
	delete(refreshingByAddress, asset)
	if len(refreshingByAddress) == 0 {
		delete(s.immediateRefreshing, addressBase58)
	}
}

func (s *BalanceService) Flush(ctx context.Context, blockNumber int64) {
	s.mu.Lock()
	pending := s.dirty
	s.dirty = make(map[string]map[string]struct{})
	s.mu.Unlock()

	tasks := make([]tronBalanceTask, 0)
	for addressBase58, assets := range pending {
		addressHex, err := tron.Base58ToHex(addressBase58)
		if err != nil {
			s.logger.Printf("convert address to hex failed: %s err=%v", addressBase58, err)
			continue
		}

		for asset := range assets {
			tasks = append(tasks, tronBalanceTask{
				addressBase58: addressBase58,
				addressHex:    addressHex,
				asset:         asset,
			})
		}
	}

	if len(tasks) == 0 {
		return
	}

	workers := tronBalanceWorkers
	if workers > len(tasks) {
		workers = len(tasks)
	}

	taskCh := make(chan tronBalanceTask, len(tasks))
	group, groupCtx := errgroup.WithContext(ctx)
	for i := 0; i < workers; i++ {
		group.Go(func() error {
			for {
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				case task, ok := <-taskCh:
					if !ok {
						return nil
					}
					s.refreshBalance(groupCtx, task, blockNumber)
				}
			}
		})
	}

	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)

	if err := group.Wait(); err != nil && err != context.Canceled {
		s.logger.Printf("flush balances failed: %v", err)
	}
}

func (s *BalanceService) RefreshAll(ctx context.Context, blockNumber int64) {
	addresses := s.cache.List()
	for _, address := range addresses {
		s.Mark(address, "TRX")
		s.Mark(address, "USDT")
	}
	s.Flush(ctx, blockNumber)
}

func (s *BalanceService) RefreshAddresses(ctx context.Context, addresses []string) error {
	blockNumber, err := s.tronClient.GetHeadBlockNumber(ctx)
	if err != nil {
		return err
	}

	seen := make(map[string]struct{}, len(addresses))
	for _, addressBase58 := range addresses {
		addressBase58 = strings.TrimSpace(addressBase58)
		if addressBase58 == "" {
			continue
		}
		if _, ok := seen[addressBase58]; ok {
			continue
		}
		seen[addressBase58] = struct{}{}

		addressHex, err := tron.Base58ToHex(addressBase58)
		if err != nil {
			s.logger.Printf("convert address to hex failed: %s err=%v", addressBase58, err)
			continue
		}

		s.refreshBalance(ctx, tronBalanceTask{
			addressBase58: addressBase58,
			addressHex:    addressHex,
			asset:         "TRX",
		}, blockNumber)
		s.refreshBalance(ctx, tronBalanceTask{
			addressBase58: addressBase58,
			addressHex:    addressHex,
			asset:         "USDT",
		}, blockNumber)
	}
	return nil
}

func (s *BalanceService) RefreshAddressesWithPositiveTRX(ctx context.Context, addresses []string) error {
	blockNumber, err := s.tronClient.GetHeadBlockNumber(ctx)
	if err != nil {
		return err
	}

	seen := make(map[string]struct{}, len(addresses))
	for _, addressBase58 := range addresses {
		addressBase58 = strings.TrimSpace(addressBase58)
		if addressBase58 == "" {
			continue
		}
		if _, ok := seen[addressBase58]; ok {
			continue
		}
		seen[addressBase58] = struct{}{}

		addressHex, err := tron.Base58ToHex(addressBase58)
		if err != nil {
			s.logger.Printf("convert address to hex failed: %s err=%v", addressBase58, err)
			continue
		}

		active, trxBalance, err := s.tronClient.GetAccountState(ctx, addressHex)
		if err != nil {
			s.logger.Printf("refresh tron account state failed during manual full refresh: %s err=%v", addressBase58, err)
			continue
		}
		if !active || !trxBalance.GreaterThan(decimal.Zero) {
			s.logger.Printf("manual full refresh skipped: address=%s active=%t trx_balance=%s", addressBase58, active, trxBalance.String())
			continue
		}

		if err := s.repo.UpsertBalance(ctx, addressBase58, "TRX", trxBalance, blockNumber); err != nil {
			s.logger.Printf("save trx balance failed during manual full refresh: %s err=%v", addressBase58, err)
			continue
		}
		s.logger.Printf("balance updated: address=%s asset=TRX balance=%s block=%d", addressBase58, trxBalance.String(), blockNumber)

		usdtBalance, err := s.tronClient.GetUSDTBalance(ctx, addressHex)
		if err != nil {
			s.logger.Printf("refresh usdt balance failed during manual full refresh: %s err=%v", addressBase58, err)
			continue
		}

		currentDBBalance, balanceErr := s.getCurrentUSDTBalance(ctx, addressBase58)
		if balanceErr != nil {
			s.logger.Printf("load current usdt balance failed: %s err=%v", addressBase58, balanceErr)
		}
		if err := s.repo.UpsertBalance(ctx, addressBase58, "USDT", usdtBalance, blockNumber); err != nil {
			s.logger.Printf("save usdt balance failed during manual full refresh: %s err=%v", addressBase58, err)
			continue
		}
		s.logger.Printf("balance updated: address=%s asset=USDT balance=%s block=%d", addressBase58, usdtBalance.String(), blockNumber)
		if balanceErr == nil {
			s.syncRecentUSDTTransfersIfNeeded(ctx, addressBase58, addressHex, currentDBBalance, usdtBalance)
		}
	}
	return nil
}

func (s *BalanceService) RefreshAllThrottled(ctx context.Context, blockNumber int64, perCallDelay time.Duration) {
	addresses := s.cache.List()
	for _, addressBase58 := range addresses {
		select {
		case <-ctx.Done():
			return
		default:
		}

		addressHex, err := tron.Base58ToHex(addressBase58)
		if err != nil {
			s.logger.Printf("convert address to hex failed: %s err=%v", addressBase58, err)
			continue
		}

		active, trxBalance, err := s.tronClient.GetAccountState(ctx, addressHex)
		if err != nil {
			s.logger.Printf("refresh tron account state failed: %s err=%v", addressBase58, err)
			continue
		}
		if !active {
			s.logger.Printf("skip hourly balance refresh for inactive tron address: address=%s block=%d", addressBase58, blockNumber)
			continue
		}

		if err := s.repo.UpsertBalance(ctx, addressBase58, "TRX", trxBalance, blockNumber); err != nil {
			s.logger.Printf("save trx balance failed: %s err=%v", addressBase58, err)
		} else {
			s.logger.Printf("balance updated: address=%s asset=TRX balance=%s block=%d source=onchain", addressBase58, trxBalance.String(), blockNumber)
		}
		if perCallDelay > 0 {
			timer := time.NewTimer(perCallDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}

		usdtBalance, err := s.tronClient.GetUSDTBalance(ctx, addressHex)
		if err != nil {
			s.logger.Printf("refresh usdt balance failed: %s err=%v", addressBase58, err)
		} else {
			currentDBBalance, balanceErr := s.getCurrentUSDTBalance(ctx, addressBase58)
			if balanceErr != nil {
				s.logger.Printf("load current usdt balance failed: %s err=%v", addressBase58, balanceErr)
			}
			if err := s.repo.UpsertBalance(ctx, addressBase58, "USDT", usdtBalance, blockNumber); err != nil {
				s.logger.Printf("save usdt balance failed: %s err=%v", addressBase58, err)
			} else {
				s.logger.Printf("balance updated: address=%s asset=USDT balance=%s block=%d source=onchain", addressBase58, usdtBalance.String(), blockNumber)
				if balanceErr == nil {
					s.syncRecentUSDTTransfersIfNeeded(ctx, addressBase58, addressHex, currentDBBalance, usdtBalance)
				}
			}
		}
		if perCallDelay > 0 {
			timer := time.NewTimer(perCallDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}

func (s *BalanceService) refreshBalance(ctx context.Context, task tronBalanceTask, blockNumber int64) {
	// #region debug-point C:refresh-balance-start
	balanceDebugReport("C", "balance.refresh.start", map[string]any{
		"address":     task.addressBase58,
		"asset":       task.asset,
		"block":       blockNumber,
		"prefer_head": task.preferHead,
	})
	// #endregion
	switch task.asset {
	case "TRX":
		active, balance, err := s.loadTRXBalance(ctx, task)
		if err != nil {
			// #region debug-point C:refresh-trx-failed
			balanceDebugReport("C", "balance.refresh.trx.failed", map[string]any{
				"address":     task.addressBase58,
				"block":       blockNumber,
				"prefer_head": task.preferHead,
				"err":         err.Error(),
			})
			// #endregion
			s.logger.Printf("refresh trx balance failed: %s err=%v", task.addressBase58, err)
			return
		}
		if err := s.repo.UpsertBalance(ctx, task.addressBase58, "TRX", balance, blockNumber); err != nil {
			// #region debug-point D:refresh-trx-save-failed
			balanceDebugReport("D", "balance.refresh.trx.save_failed", map[string]any{
				"address": task.addressBase58,
				"block":   blockNumber,
				"balance": balance.String(),
				"err":     err.Error(),
			})
			// #endregion
			s.logger.Printf("save trx balance failed: %s err=%v", task.addressBase58, err)
			return
		}
		if row, ok, rowErr := s.repo.GetDashboardRowByAddress(ctx, task.addressBase58); rowErr == nil && ok && row != nil {
			// #region debug-point D:refresh-trx-saved
			balanceDebugReport("D", "balance.refresh.trx.saved", map[string]any{
				"address":       task.addressBase58,
				"block":         blockNumber,
				"active":        active,
				"queried":       balance.String(),
				"persisted_trx": row.TRXBalance.String(),
				"persisted_usdt": row.USDTBalance.String(),
			})
			// #endregion
		}
		if !active {
			s.logger.Printf("balance updated: address=%s asset=TRX balance=%s block=%d inactive=true source=onchain", task.addressBase58, balance.String(), blockNumber)
			return
		}
		s.logger.Printf("balance updated: address=%s asset=TRX balance=%s block=%d source=onchain", task.addressBase58, balance.String(), blockNumber)
	case "USDT":
		balance, err := s.tronClient.GetUSDTBalance(ctx, task.addressHex)
		if err != nil {
			// #region debug-point C:refresh-usdt-failed
			balanceDebugReport("C", "balance.refresh.usdt.failed", map[string]any{
				"address": task.addressBase58,
				"block":   blockNumber,
				"err":     err.Error(),
			})
			// #endregion
			s.logger.Printf("refresh usdt balance failed: %s err=%v", task.addressBase58, err)
			return
		}
		currentDBBalance, balanceErr := s.getCurrentUSDTBalance(ctx, task.addressBase58)
		if balanceErr != nil {
			s.logger.Printf("load current usdt balance failed: %s err=%v", task.addressBase58, balanceErr)
		}
		if err := s.repo.UpsertBalance(ctx, task.addressBase58, "USDT", balance, blockNumber); err != nil {
			// #region debug-point D:refresh-usdt-save-failed
			balanceDebugReport("D", "balance.refresh.usdt.save_failed", map[string]any{
				"address": task.addressBase58,
				"block":   blockNumber,
				"balance": balance.String(),
				"err":     err.Error(),
			})
			// #endregion
			s.logger.Printf("save usdt balance failed: %s err=%v", task.addressBase58, err)
			return
		}
		if row, ok, rowErr := s.repo.GetDashboardRowByAddress(ctx, task.addressBase58); rowErr == nil && ok && row != nil {
			// #region debug-point D:refresh-usdt-saved
			balanceDebugReport("D", "balance.refresh.usdt.saved", map[string]any{
				"address":        task.addressBase58,
				"block":          blockNumber,
				"queried":        balance.String(),
				"persisted_trx":  row.TRXBalance.String(),
				"persisted_usdt": row.USDTBalance.String(),
			})
			// #endregion
		}
		s.logger.Printf("balance updated: address=%s asset=USDT balance=%s block=%d source=onchain", task.addressBase58, balance.String(), blockNumber)
		if balanceErr == nil {
			s.syncRecentUSDTTransfersIfNeeded(ctx, task.addressBase58, task.addressHex, currentDBBalance, balance)
		}
	}
}

func (s *BalanceService) loadTRXBalance(ctx context.Context, task tronBalanceTask) (bool, decimal.Decimal, error) {
	if task.preferHead {
		active, balance, err := s.tronClient.GetAccountStateHead(ctx, task.addressHex)
		if err == nil {
			// #region debug-point B:trx-head-read
			balanceDebugReport("B", "balance.trx.head.read", map[string]any{
				"address": task.addressBase58,
				"active":  active,
				"balance": balance.String(),
			})
			// #endregion
		}
		return active, balance, err
	}
	active, balance, err := s.tronClient.GetAccountState(ctx, task.addressHex)
	if err == nil {
		// #region debug-point B:trx-solidity-read
		balanceDebugReport("B", "balance.trx.solidity.read", map[string]any{
			"address": task.addressBase58,
			"active":  active,
			"balance": balance.String(),
		})
		// #endregion
	}
	return active, balance, err
}

// #region debug-point A:balance-debug-report
func balanceDebugReport(hypothesisID string, msg string, data map[string]any) {
	go func() {
		envPath := ".dbg/tron-head-balance-lag.env"
		url := "http://127.0.0.1:7777/event"
		sessionID := "tron-head-balance-lag"
		if b, err := os.ReadFile(envPath); err == nil {
			for _, line := range strings.Split(string(b), "\n") {
				if strings.HasPrefix(line, "DEBUG_SERVER_URL=") {
					url = strings.TrimSpace(strings.TrimPrefix(line, "DEBUG_SERVER_URL="))
				}
				if strings.HasPrefix(line, "DEBUG_SESSION_ID=") {
					sessionID = strings.TrimSpace(strings.TrimPrefix(line, "DEBUG_SESSION_ID="))
				}
			}
		}
		payload := map[string]any{
			"sessionId":    sessionID,
			"runId":        "pre",
			"hypothesisId": hypothesisID,
			"location":     "balance_service.go",
			"msg":          "[DEBUG] " + msg,
			"data":         data,
			"ts":           time.Now().UnixMilli(),
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			return
		}
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{Timeout: 800 * time.Millisecond}
		resp, err := client.Do(req)
		if err != nil {
			return
		}
		_ = resp.Body.Close()
	}()
}

// #endregion

func (s *BalanceService) getCurrentUSDTBalance(ctx context.Context, addressBase58 string) (decimal.Decimal, error) {
	row, ok, err := s.repo.GetDashboardRowByAddress(ctx, addressBase58)
	if err != nil {
		return decimal.Zero, err
	}
	if ok && row != nil {
		return row.USDTBalance, nil
	}
	return decimal.Zero, nil
}

func (s *BalanceService) syncRecentUSDTTransfersIfNeeded(ctx context.Context, addressBase58, addressHex string, currentDBBalance, latestBalance decimal.Decimal) {
	if latestBalance.Sub(currentDBBalance).Abs().LessThanOrEqual(usdtTransferRepairThreshold) {
		return
	}
	if ctx.Err() != nil {
		return
	}
	if !s.tryStartUSDTRepair(addressBase58) {
		s.logger.Printf("skip tron usdt repair sync: address=%s old_balance=%s new_balance=%s reason=repair_inflight",
			addressBase58, currentDBBalance.String(), latestBalance.String())
		return
	}

	go func() {
		defer s.finishUSDTRepair(addressBase58)

		repairCtx, cancel := context.WithTimeout(context.Background(), tronUSDTRepairTimeout)
		defer cancel()

		insertedIn, insertedOut, err := s.syncRecentTronUSDTTransfers(repairCtx, addressBase58, addressHex, time.Now().Add(-tronUSDTRepairLookback))
		if err != nil {
			s.logger.Printf("repair tron usdt transfers failed: address=%s old_balance=%s new_balance=%s err=%v", addressBase58, currentDBBalance.String(), latestBalance.String(), err)
			return
		}
		s.logger.Printf("repair tron usdt transfers done: address=%s old_balance=%s new_balance=%s inserted_in=%d inserted_out=%d",
			addressBase58, currentDBBalance.String(), latestBalance.String(), insertedIn, insertedOut)
	}()
}

func (s *BalanceService) tryStartUSDTRepair(addressBase58 string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.usdtRepairing[addressBase58]; exists {
		return false
	}
	s.usdtRepairing[addressBase58] = struct{}{}
	return true
}

func (s *BalanceService) finishUSDTRepair(addressBase58 string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.usdtRepairing, addressBase58)
}

func (s *BalanceService) syncRecentTronUSDTTransfers(ctx context.Context, watchAddressBase58, watchAddressHex string, since time.Time) (int, int, error) {
	headBlock, err := s.tronClient.GetHeadBlockNumber(ctx)
	if err != nil {
		return 0, 0, err
	}

	sinceMillis := since.UnixMilli()
	insertedIn := 0
	insertedOut := 0
	for blockNum := headBlock; blockNum > 0; blockNum-- {
		select {
		case <-ctx.Done():
			return insertedIn, insertedOut, ctx.Err()
		default:
		}

		block, err := s.tronClient.GetBlockByNum(ctx, blockNum)
		if err != nil {
			return insertedIn, insertedOut, err
		}
		if block.BlockHeader.RawData.Timestamp < sinceMillis {
			break
		}

		for _, tx := range block.Transactions {
			if s == nil || s.tronClient == nil {
				continue
			}
			if !s.tronClient.ShouldInspectUSDTTriggerTx(tx, func(hexAddr string) bool {
				return strings.EqualFold(tron.NormalizeHexAddress(hexAddr), tron.NormalizeHexAddress(watchAddressHex))
			}) {
				continue
			}

			txInfo, err := s.tronClient.GetTransactionInfoByID(ctx, tx.TxID)
			if err != nil {
				s.logger.Printf("load tron tx info failed during usdt repair sync: address=%s tx=%s err=%v", watchAddressBase58, tx.TxID, err)
				continue
			}

			for idx, logItem := range txInfo.Log {
				if !s.tronClient.IsUSDTTransferLog(logItem) {
					continue
				}

				fromHex, toHex, amount, err := s.tronClient.DecodeTransferLog(logItem)
				if err != nil {
					s.logger.Printf("decode tron usdt transfer failed during repair sync: address=%s tx=%s err=%v", watchAddressBase58, tx.TxID, err)
					continue
				}

				matchFrom := strings.EqualFold(tron.NormalizeHexAddress(fromHex), tron.NormalizeHexAddress(watchAddressHex))
				matchTo := strings.EqualFold(tron.NormalizeHexAddress(toHex), tron.NormalizeHexAddress(watchAddressHex))
				if !matchFrom && !matchTo {
					continue
				}

				record := repository.TransferRecord{
					TxHash:      tx.TxID,
					BlockNumber: blockNum,
					BlockTime:   block.BlockHeader.RawData.Timestamp,
					AssetCode:   "USDT",
					ContractAddress: sqlNullString(
						tron.NormalizeHexAddress(logItem.Address),
					),
					WatchAddress: watchAddressBase58,
					FromAddress:  fallbackBase58(fromHex, ""),
					ToAddress:    fallbackBase58(toHex, ""),
					Amount:       amount,
					LogIndex:     idx,
					Status:       "CONFIRMED",
				}

				if matchFrom {
					inserted, err := s.repo.InsertTransferOutIfAbsent(ctx, record)
					if err != nil {
						s.logger.Printf("insert tron transfer out failed during repair sync: address=%s tx=%s err=%v", watchAddressBase58, tx.TxID, err)
					} else if inserted {
						insertedOut++
					}
				}
				if matchTo {
					inserted, err := s.repo.InsertTransferInIfAbsent(ctx, record)
					if err != nil {
						s.logger.Printf("insert tron transfer in failed during repair sync: address=%s tx=%s err=%v", watchAddressBase58, tx.TxID, err)
					} else if inserted {
						insertedIn++
					}
				}
			}
		}
	}

	return insertedIn, insertedOut, nil
}

func isTriggerSmartContractTx(tx tron.Transaction) bool {
	if len(tx.RawData.Contract) == 0 {
		return false
	}
	return tx.RawData.Contract[0].Type == "TriggerSmartContract"
}

func sqlNullString(value string) sql.NullString {
	return sql.NullString{
		String: value,
		Valid:  strings.TrimSpace(value) != "",
	}
}
