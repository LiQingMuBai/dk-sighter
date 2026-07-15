package service

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"

	"tron_watcher/internal/bsc"
	"tron_watcher/internal/repository"
)

const bscSyncKey = "bsc_scanner"
const bscBalanceWorkers = 10
const bscProgressLogInterval int64 = 10

type maxScanBlockResolver func(context.Context, int64) (int64, bool, error)

type BSCScanner struct {
	client                     *bsc.Client
	repo                       *repository.DB
	cache                      *BSCAddressCache
	notifier                   TransferNotifier
	syncKey                    string
	startBlock                 int64
	confirmations              int
	insertIfAbsent             bool
	logger                     *log.Logger
	maxScanBlock               maxScanBlockResolver
	skipToLatest               bool
	deferBalanceRefreshInCatch bool
	syncGapChain               string
	disableUSDTRepair          bool
	fastCatchUpThreshold       int64
	fastCatchUpActive          bool

	triggerCh chan struct{}
	runMu     sync.Mutex
}

func NewBSCScanner(client *bsc.Client, repo *repository.DB, cache *BSCAddressCache, notifier TransferNotifier, startBlock int64, confirmations int) *BSCScanner {
	return NewBSCScannerWithSyncKey(client, repo, cache, notifier, startBlock, confirmations, "", false)
}

func NewBSCScannerWithSyncKey(client *bsc.Client, repo *repository.DB, cache *BSCAddressCache, notifier TransferNotifier, startBlock int64, confirmations int, syncKeyOverride string, insertIfAbsent bool) *BSCScanner {
	if confirmations < 0 {
		confirmations = 0
	}
	syncKey := bscSyncKey
	if value := strings.TrimSpace(syncKeyOverride); value != "" {
		syncKey = value
	}
	return &BSCScanner{
		client:         client,
		repo:           repo,
		cache:          cache,
		notifier:       notifier,
		syncKey:        syncKey,
		startBlock:     startBlock,
		confirmations:  confirmations,
		insertIfAbsent: insertIfAbsent,
		logger:         bscLogger(),
		skipToLatest:   true,
		triggerCh:      make(chan struct{}, 1),
	}
}

func (s *BSCScanner) SetLogger(logger *log.Logger) {
	if s == nil || logger == nil {
		return
	}
	s.logger = logger
}

func (s *BSCScanner) SetMaxScanBlockResolver(resolver func(context.Context, int64) (int64, bool, error)) {
	if s == nil {
		return
	}
	s.maxScanBlock = resolver
}

func (s *BSCScanner) SetSkipToLatestOnLag(enabled bool) {
	if s == nil {
		return
	}
	s.skipToLatest = enabled
}

func (s *BSCScanner) SetDeferBalanceRefreshInCatchUp(enabled bool) {
	if s == nil {
		return
	}
	s.deferBalanceRefreshInCatch = enabled
}

func (s *BSCScanner) SetSyncGapChain(chain string) {
	if s == nil {
		return
	}
	s.syncGapChain = strings.TrimSpace(chain)
}

func (s *BSCScanner) SetDisableUSDTRepair(disabled bool) {
	if s == nil {
		return
	}
	s.disableUSDTRepair = disabled
}

func (s *BSCScanner) SetFastCatchUpThreshold(threshold int64) {
	if s == nil {
		return
	}
	if threshold < 0 {
		threshold = 0
	}
	s.fastCatchUpThreshold = threshold
}

func (s *BSCScanner) RefreshAllBalances(ctx context.Context) {
	addrs := s.cache.List()
	s.refreshBalances(ctx, addrs, true)
}

func (s *BSCScanner) RefreshAddresses(ctx context.Context, addresses []string) {
	normalized := make([]string, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		address = strings.ToLower(strings.TrimSpace(address))
		if address == "" {
			continue
		}
		if _, ok := seen[address]; ok {
			continue
		}
		seen[address] = struct{}{}
		normalized = append(normalized, address)
	}
	s.refreshBalances(ctx, normalized, true)
}

func (s *BSCScanner) RefreshAllBalancesThrottled(ctx context.Context, perCallDelay time.Duration) {
	addresses := s.cache.List()
	for _, address := range addresses {
		select {
		case <-ctx.Done():
			return
		default:
		}

		address = strings.ToLower(strings.TrimSpace(address))
		if address == "" {
			continue
		}

		bnb, err := s.client.GetBNBBalance(ctx, address)
		if err != nil {
			s.logger.Printf("refresh bnb balance failed: %s err=%v", address, err)
		} else {
			_ = repository.UpsertBSCBalance(ctx, s.repo, address, "BNB", normalizeDecimal(bnb))
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

		usdt, err := s.client.GetUSDTBalance(ctx, address)
		if err != nil {
			s.logger.Printf("refresh usdt balance failed: %s err=%v", address, err)
		} else {
			currentDBBalance, balanceErr := s.getCurrentUSDTBalance(ctx, address)
			if balanceErr != nil {
				s.logger.Printf("load current bsc usdt balance failed: %s err=%v", address, balanceErr)
			}
			_ = repository.UpsertBSCBalance(ctx, s.repo, address, "USDT", normalizeDecimal(usdt))
			if balanceErr == nil {
				s.syncRecentUSDTTransfersIfNeeded(ctx, address, currentDBBalance, usdt)
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

func (s *BSCScanner) Trigger() {
	select {
	case s.triggerCh <- struct{}{}:
	default:
	}
}

func (s *BSCScanner) Run(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s.Trigger()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.Trigger()
		case <-s.triggerCh:
			if err := s.scan(ctx); err != nil {
				s.logger.Printf("scan failed: %v", err)
			}
		}
	}
}

func (s *BSCScanner) RunTriggered(ctx context.Context) error {
	s.Trigger()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.triggerCh:
			if err := s.scan(ctx); err != nil {
				s.logger.Printf("scan failed: %v", err)
			}
		}
	}
}

func (s *BSCScanner) scan(ctx context.Context) error {
	s.runMu.Lock()
	defer s.runMu.Unlock()

	lastBlock, exists, err := s.repo.GetLastBlock(ctx, s.syncKey)
	if err != nil {
		return err
	}

	if !exists {
		latest, err := s.client.BlockNumber(ctx)
		if err != nil {
			return err
		}
		latestInt := int64(latest)
		if latestInt < 0 {
			return nil
		}
		if s.confirmations > 0 && latestInt > int64(s.confirmations) {
			latestInt = latestInt - int64(s.confirmations)
		}

		initial := latestInt
		if s.startBlock > 0 {
			initial = s.startBlock
			if initial > latestInt {
				initial = latestInt
			}
		}
		if err := s.repo.SaveLastBlock(ctx, s.syncKey, initial); err != nil {
			return err
		}

		addrs := s.cache.List()
		if len(addrs) > 0 {
			s.logger.Printf("scanner state: head=%d db_last=<none> start_block=%d", latestInt, initial)
			s.logger.Printf("scanner initialized: start_block=%d latest=%d addresses=%d", initial, latestInt, len(addrs))
			s.refreshBalances(ctx, addrs, true)
		} else {
			s.logger.Printf("scanner state: head=%d db_last=<none> start_block=%d", latestInt, initial)
			s.logger.Printf("scanner initialized: start_block=%d latest=%d addresses=0", initial, latestInt)
		}
		return nil
	}

	latest, err := s.client.BlockNumber(ctx)
	if err != nil {
		return err
	}
	latestInt := int64(latest)
	if latestInt < 0 {
		return nil
	}
	if s.confirmations > 0 && latestInt > int64(s.confirmations) {
		latestInt = latestInt - int64(s.confirmations)
	}

	scanTarget := latestInt
	if s.maxScanBlock != nil {
		resolvedTarget, ok, err := s.maxScanBlock(ctx, latestInt)
		if err != nil {
			return err
		}
		if !ok {
			s.logger.Printf("scanner waiting for external scan target: head=%d db_last=%d", latestInt, lastBlock)
			return nil
		}
		if resolvedTarget < 0 {
			resolvedTarget = 0
		}
		if resolvedTarget < scanTarget {
			scanTarget = resolvedTarget
		}
	}

	scanLag := scanTarget - lastBlock
	if scanLag < 0 {
		scanLag = 0
	}
	if s.maxScanBlock != nil {
		s.logger.Printf("scanner state: head=%d target=%d db_last=%d scan_lag=%d", latestInt, scanTarget, lastBlock, scanLag)
	} else {
		s.logger.Printf("scanner state: head=%d db_last=%d scan_lag=%d", latestInt, lastBlock, scanLag)
	}
	fastCatchUp := s.fastCatchUpThreshold > 0 && scanLag > s.fastCatchUpThreshold
	if fastCatchUp != s.fastCatchUpActive {
		s.fastCatchUpActive = fastCatchUp
		if fastCatchUp {
			s.logger.Printf("scanner fast catch-up mode enabled: scan_lag=%d threshold=%d", scanLag, s.fastCatchUpThreshold)
		} else {
			s.logger.Printf("scanner fast catch-up mode disabled: scan_lag=%d threshold=%d", scanLag, s.fastCatchUpThreshold)
		}
	}
	if scanTarget <= lastBlock {
		return nil
	}
	if s.skipToLatest && shouldSkipToLatestBlock(lastBlock, scanTarget) {
		if err := s.recordSkippedGap(ctx, lastBlock+1, scanTarget); err != nil {
			return err
		}
		s.logger.Printf("scanner lag too large, skip to latest block: db_last=%d latest=%d lag=%d threshold=%d", lastBlock, scanTarget, scanTarget-lastBlock, maxAllowedSyncLagBlocks)
		if err := s.repo.SaveLastBlock(ctx, s.syncKey, scanTarget); err != nil {
			return err
		}
		return nil
	}
	if lastBlock < scanTarget {
		s.logger.Printf("scanner catching up: from=%d to=%d", lastBlock+1, scanTarget)
	}

	currentBlock := lastBlock
	progressBaseBlock := lastBlock
	deferBalanceRefresh := s.deferBalanceRefreshInCatch && scanTarget > lastBlock
	deferredRefresh := make(map[string]struct{})
	if deferBalanceRefresh {
		s.logger.Printf("scanner deferred balance refresh enabled: from=%d to=%d", lastBlock+1, scanTarget)
	}
	for currentBlock < scanTarget {
		latestDBBlock, changed, err := resolveSyncCursor(ctx, currentBlock, func(runCtx context.Context) (int64, bool, error) {
			return s.repo.GetLastBlock(runCtx, s.syncKey)
		})
		if err != nil {
			return err
		}
		if changed {
			s.logger.Printf("scanner sync cursor updated from mysql: old=%d new=%d", currentBlock, latestDBBlock)
			currentBlock = latestDBBlock
			progressBaseBlock = latestDBBlock
			if currentBlock >= scanTarget {
				break
			}
			if s.skipToLatest && shouldSkipToLatestBlock(currentBlock, scanTarget) {
				if err := s.recordSkippedGap(ctx, currentBlock+1, scanTarget); err != nil {
					return err
				}
				s.logger.Printf("scanner lag too large after mysql cursor update, skip to latest block: db_last=%d latest=%d lag=%d threshold=%d", currentBlock, scanTarget, scanTarget-currentBlock, maxAllowedSyncLagBlocks)
				if err := s.repo.SaveLastBlock(ctx, s.syncKey, scanTarget); err != nil {
					return err
				}
				break
			}
		}

		blockNum := currentBlock + 1
		hitAddresses, err := s.scanBlock(ctx, uint64(blockNum), !deferBalanceRefresh)
		if err != nil {
			return err
		}
		if deferBalanceRefresh {
			for _, addr := range hitAddresses {
				addr = strings.ToLower(strings.TrimSpace(addr))
				if addr == "" {
					continue
				}
				deferredRefresh[addr] = struct{}{}
			}
		}
		if err := s.repo.SaveLastBlock(ctx, s.syncKey, blockNum); err != nil {
			return err
		}
		currentBlock = blockNum
		processed := currentBlock - progressBaseBlock
		if processed%bscProgressLogInterval == 0 || currentBlock == scanTarget {
			s.logger.Printf("scanner progress: current=%d target=%d processed=%d remaining=%d", currentBlock, scanTarget, processed, scanTarget-currentBlock)
		}
	}
	if deferBalanceRefresh && len(deferredRefresh) > 0 {
		addrs := make([]string, 0, len(deferredRefresh))
		for addr := range deferredRefresh {
			addrs = append(addrs, addr)
		}
		s.logger.Printf("scanner deferred balance refresh start: addresses=%d", len(addrs))
		s.refreshBalances(ctx, addrs, false)
	}

	return nil
}

func (s *BSCScanner) recordSkippedGap(ctx context.Context, fromBlock, toBlock int64) error {
	if s == nil || s.repo == nil || strings.TrimSpace(s.syncGapChain) == "" {
		return nil
	}
	if toBlock < fromBlock {
		return nil
	}
	if err := s.repo.CreateSyncGap(ctx, s.syncGapChain, s.syncKey, fromBlock, toBlock); err != nil {
		return err
	}
	s.logger.Printf("scanner skip gap recorded: sync_key=%s chain=%s from=%d to=%d", s.syncKey, s.syncGapChain, fromBlock, toBlock)
	return nil
}

func (s *BSCScanner) scanBlock(ctx context.Context, blockNum uint64, refreshBalances bool) ([]string, error) {
	var (
		block         *bsc.Block
		usdtTransfers []bsc.ERC20Transfer
	)
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		value, err := s.client.GetBlockByNumber(groupCtx, blockNum)
		if err != nil {
			return err
		}
		block = value
		return nil
	})
	group.Go(func() error {
		value, err := s.client.GetUSDTTransfersByBlock(groupCtx, blockNum)
		if err != nil {
			return err
		}
		usdtTransfers = value
		return nil
	})
	if err := group.Wait(); err != nil {
		return nil, err
	}

	hit := make(map[string]struct{})

	for _, tx := range block.Transactions {
		if tx.Value == nil || tx.Value.Sign() == 0 {
			continue
		}
		from := strings.ToLower(strings.TrimSpace(tx.From))
		to := strings.ToLower(strings.TrimSpace(tx.To))
		hitFrom := from != "" && s.cache.Has(from)
		hitTo := to != "" && s.cache.Has(to)
		if !hitFrom && !hitTo {
			continue
		}

		amount := decimal.NewFromBigInt(tx.Value, 0).Div(decimal.NewFromInt(1_000_000_000_000_000_000))
		baseRecord := repository.TransferRecord{
			TxHash:          tx.Hash,
			BlockNumber:     int64(blockNum),
			BlockTime:       int64(block.Timestamp) * 1000,
			AssetCode:       "BNB",
			ContractAddress: sql.NullString{},
			FromAddress:     from,
			ToAddress:       to,
			Amount:          amount,
			LogIndex:        0,
			Status:          "CONFIRMED",
		}
		if hitFrom {
			record := baseRecord
			record.WatchAddress = from
			s.insertTransferOut(ctx, record)
		}
		if hitTo {
			record := baseRecord
			record.WatchAddress = to
			s.insertTransferIn(ctx, record)
		}

		if hitFrom {
			hit[from] = struct{}{}
		}
		if hitTo {
			hit[to] = struct{}{}
		}
	}

	usdtContract := s.client.USDTContract()
	for _, transfer := range usdtTransfers {
		from := strings.ToLower(strings.TrimSpace(transfer.From))
		to := strings.ToLower(strings.TrimSpace(transfer.To))
		hitFrom := from != "" && s.cache.Has(from)
		hitTo := to != "" && s.cache.Has(to)
		if !hitFrom && !hitTo {
			continue
		}

		amount := decimal.NewFromBigInt(transfer.Value, 0).Div(decimal.NewFromInt(1_000_000_000_000_000_000))
		baseRecord := repository.TransferRecord{
			TxHash:      transfer.TxHash,
			BlockNumber: int64(blockNum),
			BlockTime:   int64(block.Timestamp) * 1000,
			AssetCode:   "USDT",
			ContractAddress: sql.NullString{
				String: usdtContract,
				Valid:  usdtContract != "",
			},
			FromAddress: from,
			ToAddress:   to,
			Amount:      amount,
			LogIndex:    int(transfer.LogIndex),
			Status:      "CONFIRMED",
		}
		if hitFrom {
			record := baseRecord
			record.WatchAddress = from
			s.insertTransferOut(ctx, record)
		}
		if hitTo {
			record := baseRecord
			record.WatchAddress = to
			s.insertTransferIn(ctx, record)
		}

		if hitFrom {
			hit[from] = struct{}{}
		}
		if hitTo {
			hit[to] = struct{}{}
		}
	}

	if len(hit) == 0 {
		return nil, nil
	}

	addrs := make([]string, 0, len(hit))
	for addr := range hit {
		addrs = append(addrs, addr)
	}

	s.logger.Printf("block matched: number=%d addresses=%d", blockNum, len(addrs))
	if refreshBalances {
		s.refreshBalances(ctx, addrs, false)
	}
	return addrs, nil
}

func (s *BSCScanner) insertTransferIn(ctx context.Context, record repository.TransferRecord) {
	if s.insertIfAbsent {
		inserted, err := s.repo.InsertBSCTransferInIfAbsent(ctx, record)
		if err != nil {
			s.logger.Printf("insert transfer in failed: tx=%s asset=%s err=%v", record.TxHash, record.AssetCode, err)
			return
		}
		if !inserted {
			s.logger.Printf("duplicate transfer in skipped: tx=%s asset=%s watch_address=%s log_index=%d", record.TxHash, record.AssetCode, record.WatchAddress, record.LogIndex)
			return
		}
		if s.notifier != nil {
			s.notifier.NotifyTransfer(ctx, "bsc", "IN", record)
		}
		return
	}
	if err := s.repo.InsertBSCTransferIn(ctx, record); err != nil {
		s.logger.Printf("insert transfer in failed: tx=%s asset=%s err=%v", record.TxHash, record.AssetCode, err)
		return
	}
	if s.notifier != nil {
		s.notifier.NotifyTransfer(ctx, "bsc", "IN", record)
	}
}

func (s *BSCScanner) insertTransferOut(ctx context.Context, record repository.TransferRecord) {
	if s.insertIfAbsent {
		inserted, err := s.repo.InsertBSCTransferOutIfAbsent(ctx, record)
		if err != nil {
			s.logger.Printf("insert transfer out failed: tx=%s asset=%s err=%v", record.TxHash, record.AssetCode, err)
			return
		}
		if !inserted {
			s.logger.Printf("duplicate transfer out skipped: tx=%s asset=%s watch_address=%s log_index=%d", record.TxHash, record.AssetCode, record.WatchAddress, record.LogIndex)
			return
		}
		if s.notifier != nil {
			s.notifier.NotifyTransfer(ctx, "bsc", "OUT", record)
		}
		return
	}
	if err := s.repo.InsertBSCTransferOut(ctx, record); err != nil {
		s.logger.Printf("insert transfer out failed: tx=%s asset=%s err=%v", record.TxHash, record.AssetCode, err)
		return
	}
	if s.notifier != nil {
		s.notifier.NotifyTransfer(ctx, "bsc", "OUT", record)
	}
}

func (s *BSCScanner) refreshBalances(ctx context.Context, addresses []string, includeZero bool) {
	if len(addresses) == 0 {
		return
	}

	workers := bscBalanceWorkers
	if workers > len(addresses) {
		workers = len(addresses)
	}

	addressCh := make(chan string, len(addresses))
	group, groupCtx := errgroup.WithContext(ctx)
	for i := 0; i < workers; i++ {
		group.Go(func() error {
			for {
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				case address, ok := <-addressCh:
					if !ok {
						return nil
					}
					s.refreshAddressBalances(groupCtx, address, includeZero)
				}
			}
		})
	}

	for _, address := range addresses {
		address = strings.ToLower(strings.TrimSpace(address))
		if address == "" {
			continue
		}
		addressCh <- address
	}
	close(addressCh)

	if err := group.Wait(); err != nil && err != context.Canceled {
		s.logger.Printf("refresh balances failed: %v", err)
	}
}

func (s *BSCScanner) refreshAddressBalances(ctx context.Context, address string, includeZero bool) {
	currentBalancesLoaded := true
	currentBNB, currentUSDT, err := s.getCurrentBSCBalances(ctx, address)
	if err != nil {
		s.logger.Printf("load current bsc balances failed: %s err=%v", address, err)
		currentBNB = decimal.Zero
		currentUSDT = decimal.Zero
		currentBalancesLoaded = false
	}

	bnb, err := s.client.GetBNBBalance(ctx, address)
	if err != nil {
		s.logger.Printf("refresh bnb balance failed: %s err=%v", address, err)
	} else {
		if !bnb.Equal(currentBNB) {
			if err := repository.UpsertBSCBalance(ctx, s.repo, address, "BNB", normalizeDecimal(bnb)); err != nil {
				s.logger.Printf("update bnb balance failed: %s old=%s new=%s err=%v", address, currentBNB.String(), bnb.String(), err)
			} else {
				s.logger.Printf("bnb balance updated: address=%s old=%s new=%s source=onchain", address, currentBNB.String(), bnb.String())
			}
		}
	}

	usdt, err := s.client.GetUSDTBalance(ctx, address)
	if err != nil {
		s.logger.Printf("refresh usdt balance failed: %s err=%v", address, err)
	} else {
		if !usdt.Equal(currentUSDT) {
			if err := repository.UpsertBSCBalance(ctx, s.repo, address, "USDT", normalizeDecimal(usdt)); err != nil {
				s.logger.Printf("update usdt balance failed: %s old=%s new=%s err=%v", address, currentUSDT.String(), usdt.String(), err)
			} else {
				s.logger.Printf("usdt balance updated: address=%s old=%s new=%s source=onchain", address, currentUSDT.String(), usdt.String())
			}
		}
		if currentBalancesLoaded {
			s.syncRecentUSDTTransfersIfNeeded(ctx, address, currentUSDT, usdt)
		}
	}
}

func (s *BSCScanner) getCurrentBSCBalances(ctx context.Context, address string) (decimal.Decimal, decimal.Decimal, error) {
	record, ok, err := repository.GetBSCDashboardRecordByAddress(ctx, s.repo, address)
	if err != nil {
		return decimal.Zero, decimal.Zero, err
	}
	if !ok || record == nil {
		return decimal.Zero, decimal.Zero, nil
	}

	bnb := decimal.Zero
	if value := strings.TrimSpace(record.BNB); value != "" {
		parsed, err := decimal.NewFromString(value)
		if err != nil {
			return decimal.Zero, decimal.Zero, err
		}
		bnb = parsed
	}
	usdt := decimal.Zero
	if value := strings.TrimSpace(record.USDT); value != "" {
		parsed, err := decimal.NewFromString(value)
		if err != nil {
			return decimal.Zero, decimal.Zero, err
		}
		usdt = parsed
	}
	return bnb, usdt, nil
}

func (s *BSCScanner) getCurrentUSDTBalance(ctx context.Context, address string) (decimal.Decimal, error) {
	record, ok, err := repository.GetBSCDashboardRecordByAddress(ctx, s.repo, address)
	if err != nil {
		return decimal.Zero, err
	}

	if ok && record != nil {
		value, err := decimal.NewFromString(strings.TrimSpace(record.USDT))
		if err != nil {
			return decimal.Zero, err
		}
		return value, nil
	}
	return decimal.Zero, nil
}

func (s *BSCScanner) syncRecentUSDTTransfersIfNeeded(ctx context.Context, address string, currentDBBalance, latestBalance decimal.Decimal) {
	if latestBalance.Sub(currentDBBalance).Abs().LessThanOrEqual(usdtTransferRepairThreshold) {
		return
	}
	if s.disableUSDTRepair {
		return
	}
	if s.fastCatchUpActive {
		return
	}

	insertedIn, insertedOut, err := s.syncRecentBSCUSDTTransfers(ctx, address, time.Now().Add(-5*time.Minute))
	if err != nil {
		s.logger.Printf("repair bsc usdt transfers failed: address=%s old_balance=%s new_balance=%s err=%v", address, currentDBBalance.String(), latestBalance.String(), err)
		return
	}
	s.logger.Printf("repair bsc usdt transfers done: address=%s old_balance=%s new_balance=%s inserted_in=%d inserted_out=%d",
		address, currentDBBalance.String(), latestBalance.String(), insertedIn, insertedOut)
}

func (s *BSCScanner) syncRecentBSCUSDTTransfers(ctx context.Context, watchAddress string, since time.Time) (int, int, error) {
	headBlock, err := s.client.BlockNumber(ctx)
	if err != nil {
		return 0, 0, err
	}

	watchAddress = strings.ToLower(strings.TrimSpace(watchAddress))
	sinceUnix := uint64(since.Unix())
	insertedIn := 0
	insertedOut := 0
	for blockNum := headBlock; blockNum > 0; blockNum-- {
		select {
		case <-ctx.Done():
			return insertedIn, insertedOut, ctx.Err()
		default:
		}

		block, err := s.client.GetBlockByNumber(ctx, blockNum)
		if err != nil {
			return insertedIn, insertedOut, err
		}
		if block.Timestamp < sinceUnix {
			break
		}

		transfers, err := s.client.GetUSDTTransfersByBlock(ctx, blockNum)
		if err != nil {
			return insertedIn, insertedOut, err
		}
		for _, transfer := range transfers {
			matchFrom := strings.EqualFold(strings.TrimSpace(transfer.From), watchAddress)
			matchTo := strings.EqualFold(strings.TrimSpace(transfer.To), watchAddress)
			if !matchFrom && !matchTo {
				continue
			}

			amount := decimal.NewFromBigInt(transfer.Value, 0).Div(decimal.NewFromInt(1_000_000_000_000_000_000))
			record := repository.TransferRecord{
				TxHash:      transfer.TxHash,
				BlockNumber: int64(blockNum),
				BlockTime:   int64(block.Timestamp) * 1000,
				AssetCode:   "USDT",
				ContractAddress: sql.NullString{
					String: s.client.USDTContract(),
					Valid:  s.client.USDTContract() != "",
				},
				WatchAddress: watchAddress,
				FromAddress:  strings.ToLower(strings.TrimSpace(transfer.From)),
				ToAddress:    strings.ToLower(strings.TrimSpace(transfer.To)),
				Amount:       amount,
				LogIndex:     int(transfer.LogIndex),
				Status:       "CONFIRMED",
			}

			if matchFrom {
				inserted, err := s.repo.InsertBSCTransferOutIfAbsent(ctx, record)
				if err != nil {
					s.logger.Printf("insert bsc transfer out failed during repair sync: address=%s tx=%s err=%v", watchAddress, transfer.TxHash, err)
				} else if inserted {
					insertedOut++
				}
			}
			if matchTo {
				inserted, err := s.repo.InsertBSCTransferInIfAbsent(ctx, record)
				if err != nil {
					s.logger.Printf("insert bsc transfer in failed during repair sync: address=%s tx=%s err=%v", watchAddress, transfer.TxHash, err)
				} else if inserted {
					insertedIn++
				}
			}
		}
	}

	return insertedIn, insertedOut, nil
}

func normalizeDecimal(v decimal.Decimal) string {
	return v.String()
}
