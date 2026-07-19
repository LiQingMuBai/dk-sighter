package service

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"

	"tron_watcher/internal/repository"
	"tron_watcher/internal/tron"
)

const tronBackupTargetModeGapRepair = "gap-repair"
const tronBackupTargetModeTakeover = "takeover"
const tronBackupTakeoverMainRecheckIntervalBlocks int64 = 5

type TronBackupSync struct {
	client            *tron.Client
	repo              *repository.DB
	cache             *AddressCache
	balances          *BalanceService
	mainSyncKey       string
	syncKey           string
	startBlock        int64
	txWorkers         int
	blockSource       string
	logger            *log.Logger
	mainStaleDuration time.Duration
	skipToLatest      bool

	triggerCh chan struct{}
	runMu     sync.Mutex
}

func NewTronBackupSync(client *tron.Client, repo *repository.DB, cache *AddressCache, balances *BalanceService, mainSyncKey string, startBlock int64, txWorkers int, blockSource, syncKey string, mainStaleDuration time.Duration) *TronBackupSync {
	if txWorkers <= 0 {
		txWorkers = 1
	}
	if syncKey == "" {
		syncKey = syncKeyHead
		if strings.EqualFold(strings.TrimSpace(blockSource), "solid") {
			syncKey = syncKeySolid
		}
	}
	return &TronBackupSync{
		client:            client,
		repo:              repo,
		cache:             cache,
		balances:          balances,
		mainSyncKey:       strings.TrimSpace(mainSyncKey),
		syncKey:           syncKey,
		startBlock:        startBlock,
		txWorkers:         txWorkers,
		blockSource:       normalizeTronBackupBlockSource(blockSource),
		logger:            tronLogger(),
		mainStaleDuration: mainStaleDuration,
		skipToLatest:      true,
		triggerCh:         make(chan struct{}, 1),
	}
}

func normalizeTronBackupBlockSource(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "solid") {
		return "solid"
	}
	return "head"
}

func (s *TronBackupSync) Trigger() {
	select {
	case s.triggerCh <- struct{}{}:
	default:
	}
}

func (s *TronBackupSync) SetSkipToLatestOnLag(enabled bool) {
	if s == nil {
		return
	}
	s.skipToLatest = enabled
}

func (s *TronBackupSync) Run(ctx context.Context, interval time.Duration) error {
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
				s.logger.Printf("tron backup scan failed: %v", err)
			}
		}
	}
}

func (s *TronBackupSync) scan(ctx context.Context) error {
	s.runMu.Lock()
	defer s.runMu.Unlock()

	var headBlock int64
	var solidBlock int64
	var solidBlockErr error
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		value, err := s.client.GetHeadBlockNumber(groupCtx)
		if err != nil {
			return err
		}
		headBlock = value
		return nil
	})
	group.Go(func() error {
		value, err := s.client.GetSolidBlockNumber(groupCtx)
		if err != nil {
			solidBlockErr = err
			return nil
		}
		solidBlock = value
		return nil
	})
	if err := group.Wait(); err != nil {
		return err
	}
	if solidBlockErr != nil {
		if s.blockSource == "solid" {
			return solidBlockErr
		}
		solidBlock = headBlock
		s.logger.Printf("tron backup solid height probe failed, fallback to head height for state logging: head=%d err=%v", headBlock, solidBlockErr)
	}

	latestBlock := headBlock
	source := "head"
	if s.blockSource == "solid" {
		latestBlock = solidBlock
		source = "solid"
	}

	targetBlock, active, targetMode, err := s.resolveTargetBlock(ctx, latestBlock)
	if err != nil {
		return err
	}

	lastBlock, exists, err := s.repo.GetLastBlock(ctx, s.syncKey)
	if err != nil {
		return err
	}

	solidLag := headBlock - solidBlock
	if solidLag < 0 {
		solidLag = 0
	}
	if exists {
		scanLag := targetBlock - lastBlock
		if scanLag < 0 {
			scanLag = 0
		}
		s.logger.Printf("tron backup scanner state: source=%s mode=%s head=%d solid=%d solid_lag=%d db_last=%d scan_target=%d scan_lag=%d active=%t", source, targetMode, headBlock, solidBlock, solidLag, lastBlock, targetBlock, scanLag, active)
	} else {
		s.logger.Printf("tron backup scanner state: source=%s mode=%s head=%d solid=%d solid_lag=%d db_last=<none> scan_target=%d active=%t", source, targetMode, headBlock, solidBlock, solidLag, targetBlock, active)
	}
	if !active {
		return nil
	}

	if !exists {
		initialBlock := targetBlock
		if s.startBlock > 0 && s.startBlock < initialBlock {
			initialBlock = s.startBlock
		}
		if err := s.repo.SaveLastBlock(ctx, s.syncKey, initialBlock); err != nil {
			return err
		}
		s.logger.Printf("tron backup scanner initialized: source=%s start_block=%d target=%d", source, initialBlock, targetBlock)
		return nil
	}

	if targetBlock <= lastBlock {
		return nil
	}
	if s.skipToLatest && shouldSkipToLatestBlock(lastBlock, targetBlock) {
		s.logger.Printf("tron backup scanner lag too large, skip to latest block: source=%s db_last=%d target=%d lag=%d threshold=%d", source, lastBlock, targetBlock, targetBlock-lastBlock, maxAllowedSyncLagBlocks)
		return s.repo.SaveLastBlock(ctx, s.syncKey, targetBlock)
	}

	currentBlock := lastBlock
	var blocksSinceTakeoverMainRecheck int64
	for currentBlock < targetBlock {
		latestDBBlock, changed, err := resolveSyncCursor(ctx, currentBlock, func(runCtx context.Context) (int64, bool, error) {
			return s.repo.GetLastBlock(runCtx, s.syncKey)
		})
		if err != nil {
			return err
		}
		if changed {
			s.logger.Printf("tron backup scanner sync cursor updated from mysql: old=%d new=%d", currentBlock, latestDBBlock)
			currentBlock = latestDBBlock
			if s.skipToLatest && shouldSkipToLatestBlock(currentBlock, targetBlock) {
				s.logger.Printf("tron backup scanner lag too large after mysql cursor update, skip to latest block: source=%s db_last=%d target=%d lag=%d threshold=%d", source, currentBlock, targetBlock, targetBlock-currentBlock, maxAllowedSyncLagBlocks)
				if err := s.repo.SaveLastBlock(ctx, s.syncKey, targetBlock); err != nil {
					return err
				}
				break
			}
			if currentBlock >= targetBlock {
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
		currentBlock = blockNum

		if targetMode == tronBackupTargetModeTakeover {
			blocksSinceTakeoverMainRecheck++
			if blocksSinceTakeoverMainRecheck >= tronBackupTakeoverMainRecheckIntervalBlocks {
				blocksSinceTakeoverMainRecheck = 0
				recovered, err := s.stopTakeoverIfMainRecovered(ctx, currentBlock)
				if err != nil {
					return err
				}
				if recovered {
					s.Trigger()
					return nil
				}
			}
			preempted, err := s.preemptTakeoverForNewGap(ctx, currentBlock)
			if err != nil {
				return err
			}
			if preempted {
				s.Trigger()
				return nil
			}
		}
	}

	return nil
}

func (s *TronBackupSync) resolveTargetBlock(ctx context.Context, latestBlock int64) (int64, bool, string, error) {
	for {
		gap, exists, err := s.repo.GetNextOpenTronSyncGap(ctx)
		if err != nil {
			return 0, false, "", err
		}
		if !exists {
			break
		}

		backupBlock, backupExists, err := s.repo.GetLastBlock(ctx, s.syncKey)
		if err != nil {
			return 0, false, "", err
		}
		if backupExists && backupBlock >= gap.ToBlock {
			if err := s.repo.MarkTronSyncGapDone(ctx, gap.ID); err != nil {
				return 0, false, "", err
			}
			s.logger.Printf("tron backup gap repair finished: gap_id=%d gap_from=%d gap_to=%d backup_block=%d", gap.ID, gap.FromBlock, gap.ToBlock, backupBlock)
			continue
		}

		if gap.Status != "repairing" {
			if err := s.repo.MarkTronSyncGapRepairing(ctx, gap.ID); err != nil {
				return 0, false, "", err
			}
		}

		if !backupExists || backupBlock < gap.FromBlock-1 || backupBlock > gap.ToBlock {
			resetTo := gap.FromBlock - 1
			if resetTo < 0 {
				resetTo = 0
			}
			if err := s.repo.SaveLastBlock(ctx, s.syncKey, resetTo); err != nil {
				return 0, false, "", err
			}
		}

		s.logger.Printf("tron backup is repairing main gap: gap_id=%d gap_from=%d gap_to=%d", gap.ID, gap.FromBlock, gap.ToBlock)
		return gap.ToBlock, true, tronBackupTargetModeGapRepair, nil
	}

	if strings.TrimSpace(s.mainSyncKey) == "" {
		return 0, false, "", nil
	}

	_, updatedAt, exists, err := s.repo.GetSyncState(ctx, s.mainSyncKey)
	if err != nil {
		return 0, false, "", err
	}
	if !exists {
		s.logger.Printf("tron backup enters takeover mode: main sync cursor missing")
		return latestBlock, true, tronBackupTargetModeTakeover, nil
	}
	if s.mainStaleDuration > 0 && !updatedAt.IsZero() && time.Since(updatedAt) > s.mainStaleDuration {
		s.logger.Printf("tron backup enters takeover mode: main sync cursor stale for %s", time.Since(updatedAt).Truncate(time.Second))
		return latestBlock, true, tronBackupTargetModeTakeover, nil
	}

	return 0, false, "", nil
}

func (s *TronBackupSync) stopTakeoverIfMainRecovered(ctx context.Context, currentBlock int64) (bool, error) {
	if strings.TrimSpace(s.mainSyncKey) == "" {
		return false, nil
	}

	_, updatedAt, exists, err := s.repo.GetSyncState(ctx, s.mainSyncKey)
	if err != nil {
		return false, fmt.Errorf("check main sync state during takeover: %w", err)
	}
	if !exists {
		return false, nil
	}
	if s.mainStaleDuration > 0 && !updatedAt.IsZero() && time.Since(updatedAt) > s.mainStaleDuration {
		return false, nil
	}

	freshFor := time.Duration(0)
	if !updatedAt.IsZero() {
		freshFor = time.Since(updatedAt).Truncate(time.Second)
		if freshFor < 0 {
			freshFor = 0
		}
	}
	s.logger.Printf("tron backup takeover stopped after main recovered: current_block=%d main_sync_key=%s main_updated_ago=%s", currentBlock, s.mainSyncKey, freshFor)
	return true, nil
}

func (s *TronBackupSync) preemptTakeoverForNewGap(ctx context.Context, currentBlock int64) (bool, error) {
	gap, exists, err := s.repo.GetNextOpenTronSyncGap(ctx)
	if err != nil {
		return false, fmt.Errorf("check tron gap during takeover: %w", err)
	}
	if !exists || gap == nil {
		return false, nil
	}
	if currentBlock >= gap.ToBlock {
		return false, nil
	}
	s.logger.Printf("tron backup takeover preempted by new gap: current_block=%d gap_id=%d gap_from=%d gap_to=%d gap_status=%s", currentBlock, gap.ID, gap.FromBlock, gap.ToBlock, gap.Status)
	return true, nil
}

func (s *TronBackupSync) scanBlock(ctx context.Context, blockNum int64) error {
	block, err := s.client.GetBlockByNumWithSource(ctx, blockNum, s.blockSource)
	if err != nil {
		return err
	}

	blockTime := block.BlockHeader.RawData.Timestamp
	txCount := len(block.Transactions)
	if txCount == 0 {
		s.logger.Printf("tron backup scanning block: number=%d txs=0", blockNum)
		return nil
	}

	workers := s.txWorkers
	if workers > txCount {
		workers = txCount
	}
	s.logger.Printf("tron backup scanning block: number=%d txs=%d workers=%d", blockNum, txCount, workers)

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

func (s *TronBackupSync) processTransaction(ctx context.Context, tx tron.Transaction, blockNum, blockTime int64) {
	s.handleTRXTransfer(ctx, tx, blockNum, blockTime)
	if s.shouldInspectUSDTTriggerTx(tx) {
		s.handleUSDTTransfers(ctx, tx.TxID, blockNum, blockTime)
	}
}

func (s *TronBackupSync) shouldInspectUSDTTriggerTx(tx tron.Transaction) bool {
	if s == nil || s.client == nil {
		return false
	}
	return s.client.ShouldInspectUSDTTriggerTx(tx, func(hexAddr string) bool {
		if s.cache == nil {
			return false
		}
		_, ok := s.cache.Base58ByHex(hexAddr)
		return ok
	})
}

func (s *TronBackupSync) handleTRXTransfer(ctx context.Context, tx tron.Transaction, blockNum, blockTime int64) {
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
			s.logger.Printf("insert backup trx transfer out failed: tx=%s err=%v", tx.TxID, err)
		}
	}
	if okTo {
		inRecord := record
		inRecord.WatchAddress = toBase58
		if err := s.repo.InsertTransferIn(ctx, inRecord); err != nil {
			s.logger.Printf("insert backup trx transfer in failed: tx=%s err=%v", tx.TxID, err)
		}
	}

	s.logger.Printf("tron backup trx transfer matched: tx=%s block=%d from=%s to=%s amount=%s hit_from=%t hit_to=%t",
		tx.TxID, blockNum, record.FromAddress, record.ToAddress, record.Amount.String(), okFrom, okTo)

	if okFrom {
		s.triggerImmediateBalanceRefresh(fromBase58, tx.TxID, blockNum, "TRX", "OUT")
	}
	if okTo {
		s.triggerImmediateBalanceRefresh(toBase58, tx.TxID, blockNum, "TRX", "IN")
	}
}

func (s *TronBackupSync) handleUSDTTransfers(ctx context.Context, txID string, blockNum, blockTime int64) {
	info, err := s.client.GetTransactionInfoByIDWithSource(ctx, txID, s.blockSource)
	if err != nil {
		s.logger.Printf("load backup tx info failed: tx=%s err=%v", txID, err)
		return
	}

	for idx, logItem := range info.Log {
		if !s.client.IsUSDTTransferLog(logItem) {
			continue
		}

		fromHex, toHex, amount, err := s.client.DecodeTransferLog(logItem)
		if err != nil {
			s.logger.Printf("decode backup usdt transfer failed: tx=%s err=%v", txID, err)
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
				s.logger.Printf("insert backup usdt transfer out failed: tx=%s err=%v", txID, err)
			}
		}
		if okTo {
			inRecord := record
			inRecord.WatchAddress = toBase58
			if err := s.repo.InsertTransferIn(ctx, inRecord); err != nil {
				s.logger.Printf("insert backup usdt transfer in failed: tx=%s err=%v", txID, err)
			}
		}

		s.logger.Printf("tron backup usdt transfer matched: tx=%s block=%d from=%s to=%s amount=%s hit_from=%t hit_to=%t",
			txID, blockNum, record.FromAddress, record.ToAddress, record.Amount.String(), okFrom, okTo)

		if okFrom {
			s.triggerImmediateBalanceRefresh(fromBase58, txID, blockNum, "USDT", "OUT")
		}
		if okTo {
			s.triggerImmediateBalanceRefresh(toBase58, txID, blockNum, "USDT", "IN")
		}
	}
}

func (s *TronBackupSync) triggerImmediateBalanceRefresh(addressBase58, txID string, blockNum int64, triggerAsset, direction string) {
	if s == nil || s.balances == nil {
		return
	}
	s.balances.TriggerImmediateRefresh(addressBase58, []string{triggerAsset}, txID, blockNum, triggerAsset, direction)
}
