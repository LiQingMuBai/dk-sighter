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

	"tron_watcher/internal/repository"
	"tron_watcher/internal/tron"
)

const syncKeySolid = "tron_solid_scanner"
const syncKeyHead = "tron_head_scanner"
const tronStateWorkers = 2
const tronLatestRefreshIntervalBlocks int64 = 5

type Scanner struct {
	tronClient *tron.Client
	repo       *repository.DB
	cache      *AddressCache
	balances   *BalanceService
	notifier   TransferNotifier
	syncKey    string
	startBlock int64
	txWorkers  int
	logger     *log.Logger

	refreshAllBalancesOnTransfer bool
	skipToLatest                 bool
	syncGapChain                 string

	triggerCh chan struct{}
	runMu     sync.Mutex
}

func NewScanner(tronClient *tron.Client, repo *repository.DB, cache *AddressCache, balances *BalanceService, notifier TransferNotifier, startBlock int64, txWorkers int, tronBlockSource string) *Scanner {
	return NewScannerWithSyncKey(tronClient, repo, cache, balances, notifier, startBlock, txWorkers, tronBlockSource, "")
}

func NewScannerWithSyncKey(tronClient *tron.Client, repo *repository.DB, cache *AddressCache, balances *BalanceService, notifier TransferNotifier, startBlock int64, txWorkers int, tronBlockSource string, syncKeyOverride string) *Scanner {
	if txWorkers <= 0 {
		txWorkers = 1
	}
	syncKey := syncKeyHead
	if strings.EqualFold(strings.TrimSpace(tronBlockSource), "solid") {
		syncKey = syncKeySolid
	}
	if value := strings.TrimSpace(syncKeyOverride); value != "" {
		syncKey = value
	}
	return &Scanner{
		tronClient:   tronClient,
		repo:         repo,
		cache:        cache,
		balances:     balances,
		notifier:     notifier,
		syncKey:      syncKey,
		startBlock:   startBlock,
		txWorkers:    txWorkers,
		logger:       tronLogger(),
		skipToLatest: true,
		triggerCh:    make(chan struct{}, 1),
	}
}

func (s *Scanner) Trigger() {
	select {
	case s.triggerCh <- struct{}{}:
	default:
	}
}

func (s *Scanner) EnableFullBalanceRefreshOnTransfer(enabled bool) {
	if s == nil {
		return
	}
	s.refreshAllBalancesOnTransfer = enabled
}

func (s *Scanner) SetSkipToLatestOnLag(enabled bool) {
	if s == nil {
		return
	}
	s.skipToLatest = enabled
}

func (s *Scanner) SetSyncGapChain(chain string) {
	if s == nil {
		return
	}
	s.syncGapChain = strings.TrimSpace(chain)
}

func (s *Scanner) Run(ctx context.Context, interval time.Duration) error {
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

func (s *Scanner) scan(ctx context.Context) error {
	s.runMu.Lock()
	defer s.runMu.Unlock()

	var headBlock int64
	var latestSolid int64
	var source string
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		value, err := s.tronClient.GetHeadBlockNumber(groupCtx)
		if err != nil {
			return err
		}
		headBlock = value
		return nil
	})
	group.Go(func() error {
		value, err := s.tronClient.GetSolidBlockNumber(groupCtx)
		if err != nil {
			return err
		}
		latestSolid = value
		return nil
	})
	if err := group.Wait(); err != nil {
		return err
	}

	source = "head"
	latestBlock := headBlock
	if s.syncKey == syncKeySolid {
		source = "solid"
		latestBlock = latestSolid
	}

	lastBlock, exists, err := s.repo.GetLastBlock(ctx, s.syncKey)
	if err != nil {
		return err
	}

	solidLag := headBlock - latestSolid
	if solidLag < 0 {
		solidLag = 0
	}
	if exists {
		scanLag := latestBlock - lastBlock
		if scanLag < 0 {
			scanLag = 0
		}
		s.logger.Printf("scanner state: source=%s head=%d solid=%d solid_lag=%d db_last=%d scan_lag=%d", source, headBlock, latestSolid, solidLag, lastBlock, scanLag)
	} else {
		s.logger.Printf("scanner state: source=%s head=%d solid=%d solid_lag=%d db_last=<none>", source, headBlock, latestSolid, solidLag)
	}
	if !exists {
		initialBlock := latestBlock
		if s.startBlock > 0 {
			initialBlock = s.startBlock
			if initialBlock > latestBlock {
				initialBlock = latestBlock
			}
		}
		if err := s.repo.SaveLastBlock(ctx, s.syncKey, initialBlock); err != nil {
			return err
		}
		s.logger.Printf("scanner initialized: source=%s start_block=%d latest=%d", source, initialBlock, latestBlock)
		return nil
	}
	if s.skipToLatest && shouldSkipToLatestBlock(lastBlock, latestBlock) {
		if err := s.recordSkippedGap(ctx, lastBlock+1, latestBlock); err != nil {
			return err
		}
		s.logger.Printf("scanner lag too large, skip to latest block: source=%s db_last=%d latest=%d lag=%d threshold=%d", source, lastBlock, latestBlock, latestBlock-lastBlock, maxAllowedSyncLagBlocks)
		if err := s.repo.SaveLastBlock(ctx, s.syncKey, latestBlock); err != nil {
			return err
		}
		return nil
	}
	if lastBlock < latestBlock {
		s.logger.Printf("scanner catching up: from=%d to=%d", lastBlock+1, latestBlock)
	}

	currentBlock := lastBlock
	blocksSinceLatestRefresh := int64(0)
	for currentBlock < latestBlock {
		latestDBBlock, changed, err := resolveSyncCursor(ctx, currentBlock, func(runCtx context.Context) (int64, bool, error) {
			return s.repo.GetLastBlock(runCtx, s.syncKey)
		})
		if err != nil {
			return err
		}
		if changed {
			s.logger.Printf("scanner sync cursor updated from mysql: old=%d new=%d", currentBlock, latestDBBlock)
			currentBlock = latestDBBlock
			if s.skipToLatest && shouldSkipToLatestBlock(currentBlock, latestBlock) {
				if err := s.recordSkippedGap(ctx, currentBlock+1, latestBlock); err != nil {
					return err
				}
				s.logger.Printf("scanner lag too large after mysql cursor update, skip to latest block: source=%s db_last=%d latest=%d lag=%d threshold=%d", source, currentBlock, latestBlock, latestBlock-currentBlock, maxAllowedSyncLagBlocks)
				if err := s.repo.SaveLastBlock(ctx, s.syncKey, latestBlock); err != nil {
					return err
				}
				break
			}
			if currentBlock >= latestBlock {
				break
			}
		}

		blockNum := currentBlock + 1
		if err := s.scanBlock(ctx, blockNum); err != nil {
			return err
		}
		if err := s.repo.SaveLastBlock(ctx, s.syncKey, blockNum); err != nil {
			return err
		}
		s.balances.Flush(ctx, blockNum)
		currentBlock = blockNum
		blocksSinceLatestRefresh++

		if s.skipToLatest && blocksSinceLatestRefresh >= tronLatestRefreshIntervalBlocks {
			refreshedLatestBlock, err := s.loadLatestBlockForSource(ctx)
			if err != nil {
				return err
			}
			blocksSinceLatestRefresh = 0
			if refreshedLatestBlock != latestBlock {
				s.logger.Printf("scanner progress refresh: source=%s db_last=%d old_latest=%d new_latest=%d", source, currentBlock, latestBlock, refreshedLatestBlock)
			}
			latestBlock = refreshedLatestBlock
			if shouldSkipToLatestBlock(currentBlock, latestBlock) {
				if err := s.recordSkippedGap(ctx, currentBlock+1, latestBlock); err != nil {
					return err
				}
				s.logger.Printf("scanner lag too large after progress refresh, skip to latest block: source=%s db_last=%d latest=%d lag=%d threshold=%d", source, currentBlock, latestBlock, latestBlock-currentBlock, maxAllowedSyncLagBlocks)
				if err := s.repo.SaveLastBlock(ctx, s.syncKey, latestBlock); err != nil {
					return err
				}
				break
			}
		}
	}
	return nil
}

func (s *Scanner) loadLatestBlockForSource(ctx context.Context) (int64, error) {
	if s.syncKey == syncKeySolid {
		return s.tronClient.GetSolidBlockNumber(ctx)
	}
	return s.tronClient.GetHeadBlockNumber(ctx)
}

func (s *Scanner) scanBlock(ctx context.Context, blockNum int64) error {
	block, err := s.tronClient.GetBlockByNum(ctx, blockNum)
	if err != nil {
		return err
	}

	blockTime := block.BlockHeader.RawData.Timestamp
	txCount := len(block.Transactions)
	if txCount == 0 {
		s.logger.Printf("scanning block: number=%d txs=0", blockNum)
		return nil
	}

	workers := s.txWorkers
	if workers > txCount {
		workers = txCount
	}
	s.logger.Printf("scanning block: number=%d txs=%d workers=%d", blockNum, txCount, workers)

	txCh := make(chan tron.Transaction, txCount)
	group, groupCtx := errgroup.WithContext(ctx)

	for i := 0; i < workers; i++ {
		group.Go(func() error {
			for {
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				case tx, ok := <-txCh:
					if !ok {
						return nil
					}
					s.processTransaction(groupCtx, tx, blockNum, blockTime)
				}
			}
		})
	}

	for _, tx := range block.Transactions {
		select {
		case <-groupCtx.Done():
			close(txCh)
			_ = group.Wait()
			return groupCtx.Err()
		case txCh <- tx:
		}
	}
	close(txCh)

	if err := group.Wait(); err != nil && err != context.Canceled {
		return err
	}
	return nil
}

func (s *Scanner) processTransaction(ctx context.Context, tx tron.Transaction, blockNum, blockTime int64) {
	s.handleTRXTransfer(ctx, tx, blockNum, blockTime)
	if s.shouldInspectUSDTTriggerTx(tx) {
		s.handleUSDTTransfers(ctx, tx.TxID, blockNum, blockTime)
	}
}

func (s *Scanner) handleTRXTransfer(ctx context.Context, tx tron.Transaction, blockNum, blockTime int64) {
	if len(tx.RawData.Contract) == 0 {
		return
	}

	contract := tx.RawData.Contract[0]
	if contract.Type != "TransferContract" {
		return
	}

	fromBase58, okFrom := s.cache.Base58ByHex(contract.Parameter.Value.OwnerAddress)
	toBase58, okTo := s.cache.Base58ByHex(contract.Parameter.Value.ToAddress)
	if !okFrom && !okTo {
		return
	}

	record := repository.TransferRecord{
		TxHash:          tx.TxID,
		BlockNumber:     blockNum,
		BlockTime:       blockTime,
		AssetCode:       "TRX",
		ContractAddress: sql.NullString{},
		FromAddress:     fallbackBase58(contract.Parameter.Value.OwnerAddress, fromBase58),
		ToAddress:       fallbackBase58(contract.Parameter.Value.ToAddress, toBase58),
		Amount:          decimal.NewFromInt(contract.Parameter.Value.Amount).Div(decimal.NewFromInt(1_000_000)),
		LogIndex:        0,
		Status:          "CONFIRMED",
	}

	if okFrom {
		outRecord := record
		outRecord.WatchAddress = fromBase58
		if err := s.repo.InsertTransferOut(ctx, outRecord); err != nil {
			s.logger.Printf("insert trx transfer out failed: tx=%s err=%v", tx.TxID, err)
			return
		}
		if s.notifier != nil {
			s.notifier.NotifyTransfer(ctx, "tron", "OUT", outRecord)
		}
	}
	if okTo {
		inRecord := record
		inRecord.WatchAddress = toBase58
		if err := s.repo.InsertTransferIn(ctx, inRecord); err != nil {
			s.logger.Printf("insert trx transfer in failed: tx=%s err=%v", tx.TxID, err)
			return
		}
		if s.notifier != nil {
			s.notifier.NotifyTransfer(ctx, "tron", "IN", inRecord)
		}
	}
	s.logger.Printf("trx transfer matched: tx=%s block=%d from=%s to=%s amount=%s hit_from=%t hit_to=%t",
		tx.TxID, blockNum, record.FromAddress, record.ToAddress, record.Amount.String(), okFrom, okTo)

	if okFrom {
		s.markBalance(fromBase58, "TRX", tx.TxID, blockNum, "OUT")
	}
	if okTo {
		s.markBalance(toBase58, "TRX", tx.TxID, blockNum, "IN")
	}
}

func (s *Scanner) isSmartContractTx(tx tron.Transaction) bool {
	if len(tx.RawData.Contract) == 0 {
		return false
	}
	return tx.RawData.Contract[0].Type == "TriggerSmartContract"
}

func (s *Scanner) shouldInspectUSDTTriggerTx(tx tron.Transaction) bool {
	if s == nil || s.tronClient == nil {
		return false
	}
	return s.tronClient.ShouldInspectUSDTTriggerTx(tx, func(hexAddr string) bool {
		if s.cache == nil {
			return false
		}
		_, ok := s.cache.Base58ByHex(hexAddr)
		return ok
	})
}

func (s *Scanner) handleUSDTTransfers(ctx context.Context, txID string, blockNum, blockTime int64) {
	info, err := s.tronClient.GetTransactionInfoByID(ctx, txID)
	if err != nil {
		s.logger.Printf("load tx info failed: tx=%s err=%v", txID, err)
		return
	}

	for idx, logItem := range info.Log {
		if !s.tronClient.IsUSDTTransferLog(logItem) {
			continue
		}

		fromHex, toHex, amount, err := s.tronClient.DecodeTransferLog(logItem)
		if err != nil {
			s.logger.Printf("decode usdt transfer failed: tx=%s err=%v", txID, err)
			continue
		}

		fromBase58, okFrom := s.cache.Base58ByHex(fromHex)
		toBase58, okTo := s.cache.Base58ByHex(toHex)
		if !okFrom && !okTo {
			continue
		}

		record := repository.TransferRecord{
			TxHash:      txID,
			BlockNumber: blockNum,
			BlockTime:   blockTime,
			AssetCode:   "USDT",
			ContractAddress: sql.NullString{
				String: tron.NormalizeHexAddress(logItem.Address),
				Valid:  true,
			},
			FromAddress: fallbackBase58(fromHex, fromBase58),
			ToAddress:   fallbackBase58(toHex, toBase58),
			Amount:      amount,
			LogIndex:    idx,
			Status:      "CONFIRMED",
		}

		if okFrom {
			outRecord := record
			outRecord.WatchAddress = fromBase58
			if err := s.repo.InsertTransferOut(ctx, outRecord); err != nil {
				s.logger.Printf("insert usdt transfer out failed: tx=%s err=%v", txID, err)
				continue
			}
			if s.notifier != nil {
				s.notifier.NotifyTransfer(ctx, "tron", "OUT", outRecord)
			}
		}
		if okTo {
			inRecord := record
			inRecord.WatchAddress = toBase58
			if err := s.repo.InsertTransferIn(ctx, inRecord); err != nil {
				s.logger.Printf("insert usdt transfer in failed: tx=%s err=%v", txID, err)
				continue
			}
			if s.notifier != nil {
				s.notifier.NotifyTransfer(ctx, "tron", "IN", inRecord)
			}
		}
		s.logger.Printf("usdt transfer matched: tx=%s block=%d from=%s to=%s amount=%s hit_from=%t hit_to=%t",
			txID, blockNum, record.FromAddress, record.ToAddress, record.Amount.String(), okFrom, okTo)

		if okFrom {
			s.markBalance(fromBase58, "USDT", txID, blockNum, "OUT")
		}
		if okTo {
			s.markBalance(toBase58, "USDT", txID, blockNum, "IN")
		}
	}
}

func (s *Scanner) markBalance(addressBase58 string, asset string, txID string, blockNum int64, direction string) {
	if s == nil || s.balances == nil {
		return
	}
	if addressBase58 == "" {
		return
	}
	if s.refreshAllBalancesOnTransfer {
		s.logger.Printf("transfer matched -> refresh balances: address=%s trigger_asset=%s direction=%s tx=%s block=%d refresh_assets=TRX,USDT source=onchain",
			addressBase58, asset, direction, txID, blockNum)
		s.balances.Mark(addressBase58, "TRX")
		s.balances.Mark(addressBase58, "USDT")
		s.balances.TriggerImmediateRefresh(addressBase58, []string{"TRX", "USDT"}, txID, blockNum, asset, direction)
		return
	}
	s.balances.Mark(addressBase58, asset)
	s.balances.TriggerImmediateRefresh(addressBase58, []string{asset}, txID, blockNum, asset, direction)
}

func direction(hitFrom, hitTo bool) string {
	switch {
	case hitFrom && hitTo:
		return "SELF"
	case hitTo:
		return "IN"
	default:
		return "OUT"
	}
}

func fallbackBase58(hexAddr, base58 string) string {
	if base58 != "" {
		return base58
	}
	converted, err := tron.HexToBase58(hexAddr)
	if err != nil {
		return tron.NormalizeHexAddress(hexAddr)
	}
	return converted
}

func (s *Scanner) recordSkippedGap(ctx context.Context, fromBlock, toBlock int64) error {
	if s == nil || s.repo == nil || strings.TrimSpace(s.syncGapChain) == "" {
		return nil
	}
	if toBlock < fromBlock {
		return nil
	}
	if strings.EqualFold(strings.TrimSpace(s.syncGapChain), "tron") {
		if err := s.repo.CreateTronSyncGap(ctx, s.syncKey, fromBlock, toBlock); err != nil {
			return err
		}
	} else {
		if err := s.repo.CreateSyncGap(ctx, s.syncGapChain, s.syncKey, fromBlock, toBlock); err != nil {
			return err
		}
	}
	s.logger.Printf("scanner skip gap recorded: sync_key=%s chain=%s from=%d to=%d", s.syncKey, s.syncGapChain, fromBlock, toBlock)
	return nil
}
