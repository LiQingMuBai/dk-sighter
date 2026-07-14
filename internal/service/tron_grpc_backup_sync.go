package service

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	gotronAddress "github.com/fbsobreira/gotron-sdk/pkg/address"
	gotronAPI "github.com/fbsobreira/gotron-sdk/pkg/proto/api"
	gotronCore "github.com/fbsobreira/gotron-sdk/pkg/proto/core"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"

	"tron_watcher/internal/repository"
	"tron_watcher/internal/tron"
)

const tronGRPCTargetModeGapRepair = "gap-repair"
const tronGRPCTargetModeTakeover = "takeover"
const tronGRPCTakeoverMainRecheckIntervalBlocks int64 = 5

type TronGRPCBackupSync struct {
	client            *tron.GRPCBackupClient
	repo              *repository.DB
	cache             *AddressCache
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

func NewTronGRPCBackupSync(client *tron.GRPCBackupClient, repo *repository.DB, cache *AddressCache, mainSyncKey string, startBlock int64, txWorkers int, blockSource, syncKey string, mainStaleDuration time.Duration) *TronGRPCBackupSync {
	if txWorkers <= 0 {
		txWorkers = 1
	}
	if syncKey == "" {
		syncKey = syncKeyHead
		if blockSource == "solid" {
			syncKey = syncKeySolid
		}
	}
	return &TronGRPCBackupSync{
		client:            client,
		repo:              repo,
		cache:             cache,
		mainSyncKey:       strings.TrimSpace(mainSyncKey),
		syncKey:           syncKey,
		startBlock:        startBlock,
		txWorkers:         txWorkers,
		blockSource:       blockSource,
		logger:            tronLogger(),
		mainStaleDuration: mainStaleDuration,
		skipToLatest:      true,
		triggerCh:         make(chan struct{}, 1),
	}
}

func (s *TronGRPCBackupSync) Trigger() {
	select {
	case s.triggerCh <- struct{}{}:
	default:
	}
}

func (s *TronGRPCBackupSync) SetSkipToLatestOnLag(enabled bool) {
	if s == nil {
		return
	}
	s.skipToLatest = enabled
}

func (s *TronGRPCBackupSync) Run(ctx context.Context, interval time.Duration) error {
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
				s.logger.Printf("grpc backup scan failed: %v", err)
			}
		}
	}
}

func (s *TronGRPCBackupSync) scan(ctx context.Context) error {
	s.runMu.Lock()
	defer s.runMu.Unlock()

	var headBlock int64
	var solidBlock int64
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
			return err
		}
		solidBlock = value
		return nil
	})
	if err := group.Wait(); err != nil {
		return err
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
		s.logger.Printf("grpc backup scanner state: source=%s mode=%s head=%d solid=%d solid_lag=%d db_last=%d scan_target=%d scan_lag=%d active=%t", source, targetMode, headBlock, solidBlock, solidLag, lastBlock, targetBlock, scanLag, active)
	} else {
		s.logger.Printf("grpc backup scanner state: source=%s mode=%s head=%d solid=%d solid_lag=%d db_last=<none> scan_target=%d active=%t", source, targetMode, headBlock, solidBlock, solidLag, targetBlock, active)
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
		s.logger.Printf("grpc backup scanner initialized: source=%s start_block=%d target=%d", source, initialBlock, targetBlock)
		return nil
	}

	if targetBlock <= lastBlock {
		return nil
	}
	if s.skipToLatest && shouldSkipToLatestBlock(lastBlock, targetBlock) {
		s.logger.Printf("grpc backup scanner lag too large, skip to latest block: source=%s db_last=%d target=%d lag=%d threshold=%d", source, lastBlock, targetBlock, targetBlock-lastBlock, maxAllowedSyncLagBlocks)
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
			s.logger.Printf("grpc backup scanner sync cursor updated from mysql: old=%d new=%d", currentBlock, latestDBBlock)
			currentBlock = latestDBBlock
			if s.skipToLatest && shouldSkipToLatestBlock(currentBlock, targetBlock) {
				s.logger.Printf("grpc backup scanner lag too large after mysql cursor update, skip to latest block: source=%s db_last=%d target=%d lag=%d threshold=%d", source, currentBlock, targetBlock, targetBlock-currentBlock, maxAllowedSyncLagBlocks)
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
		if targetMode == tronGRPCTargetModeTakeover {
			blocksSinceTakeoverMainRecheck++
			if blocksSinceTakeoverMainRecheck >= tronGRPCTakeoverMainRecheckIntervalBlocks {
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

func (s *TronGRPCBackupSync) resolveTargetBlock(ctx context.Context, latestBlock int64) (int64, bool, string, error) {
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
			s.logger.Printf("tron grpc backup gap repair finished: gap_id=%d gap_from=%d gap_to=%d backup_block=%d", gap.ID, gap.FromBlock, gap.ToBlock, backupBlock)
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

		s.logger.Printf("tron grpc backup is repairing main gap: gap_id=%d gap_from=%d gap_to=%d", gap.ID, gap.FromBlock, gap.ToBlock)
		return gap.ToBlock, true, tronGRPCTargetModeGapRepair, nil
	}

	if strings.TrimSpace(s.mainSyncKey) == "" {
		return 0, false, "", nil
	}

	_, updatedAt, exists, err := s.repo.GetSyncState(ctx, s.mainSyncKey)
	if err != nil {
		return 0, false, "", err
	}
	if !exists {
		s.logger.Printf("tron grpc backup enters takeover mode: main sync cursor missing")
		return latestBlock, true, tronGRPCTargetModeTakeover, nil
	}
	if s.mainStaleDuration > 0 && !updatedAt.IsZero() && time.Since(updatedAt) > s.mainStaleDuration {
		s.logger.Printf("tron grpc backup enters takeover mode: main sync cursor stale for %s", time.Since(updatedAt).Truncate(time.Second))
		return latestBlock, true, tronGRPCTargetModeTakeover, nil
	}

	return 0, false, "", nil
}

func (s *TronGRPCBackupSync) stopTakeoverIfMainRecovered(ctx context.Context, currentBlock int64) (bool, error) {
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
	s.logger.Printf("tron grpc backup takeover stopped after main recovered: current_block=%d main_sync_key=%s main_updated_ago=%s", currentBlock, s.mainSyncKey, freshFor)
	return true, nil
}

func (s *TronGRPCBackupSync) preemptTakeoverForNewGap(ctx context.Context, currentBlock int64) (bool, error) {
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
	s.logger.Printf("tron grpc backup takeover preempted by new gap: current_block=%d gap_id=%d gap_from=%d gap_to=%d gap_status=%s", currentBlock, gap.ID, gap.FromBlock, gap.ToBlock, gap.Status)
	return true, nil
}

func (s *TronGRPCBackupSync) scanBlock(ctx context.Context, blockNum int64) error {
	block, err := s.client.GetBlockByNum(ctx, blockNum, s.blockSource)
	if err != nil {
		return err
	}

	blockTime := block.GetBlockHeader().GetRawData().GetTimestamp()
	txs := block.GetTransactions()
	if len(txs) == 0 {
		s.logger.Printf("grpc backup scanning block: number=%d txs=0", blockNum)
		return nil
	}

	workers := s.txWorkers
	if workers > len(txs) {
		workers = len(txs)
	}
	s.logger.Printf("grpc backup scanning block: number=%d txs=%d workers=%d", blockNum, len(txs), workers)

	txCh := make(chan *gotronAPI.TransactionExtention, len(txs))
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

	for _, tx := range txs {
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

func (s *TronGRPCBackupSync) processTransaction(ctx context.Context, txExt *gotronAPI.TransactionExtention, blockNum, blockTime int64) {
	if txExt == nil || txExt.GetTransaction() == nil || txExt.GetTransaction().GetRawData() == nil {
		return
	}
	txID := hex.EncodeToString(txExt.GetTxid())
	contracts := txExt.GetTransaction().GetRawData().GetContract()
	if len(contracts) == 0 {
		return
	}

	contract := contracts[0]
	switch contract.GetType() {
	case gotronCore.Transaction_Contract_TransferContract:
		transfer, err := s.client.DecodeTransferContract(contract)
		if err != nil {
			s.logger.Printf("decode trx transfer contract failed: tx=%s err=%v", txID, err)
			return
		}
		s.handleTRXTransfer(ctx, txID, transfer, blockNum, blockTime)
	case gotronCore.Transaction_Contract_TriggerSmartContract:
		s.handleUSDTTransfers(ctx, txID, blockNum, blockTime)
	}
}

func (s *TronGRPCBackupSync) handleTRXTransfer(ctx context.Context, txID string, contract *gotronCore.TransferContract, blockNum, blockTime int64) {
	if contract == nil {
		return
	}

	fromHex := tron.NormalizeHexAddress(hex.EncodeToString(contract.GetOwnerAddress()))
	toHex := tron.NormalizeHexAddress(hex.EncodeToString(contract.GetToAddress()))
	fromBase58, okFrom := s.cache.Base58ByHex(fromHex)
	toBase58, okTo := s.cache.Base58ByHex(toHex)
	if !okFrom && !okTo {
		return
	}

	record := repository.TransferRecord{
		TxHash:          txID,
		BlockNumber:     blockNum,
		BlockTime:       blockTime,
		AssetCode:       "TRX",
		ContractAddress: sql.NullString{},
		FromAddress:     fallbackBase58FromBytes(contract.GetOwnerAddress(), fromBase58),
		ToAddress:       fallbackBase58FromBytes(contract.GetToAddress(), toBase58),
		Amount:          decimal.NewFromInt(contract.GetAmount()).Div(decimal.NewFromInt(1_000_000)),
		LogIndex:        0,
		Status:          "CONFIRMED",
	}

	if okFrom {
		outRecord := record
		outRecord.WatchAddress = fromBase58
		if err := s.repo.InsertTransferOut(ctx, outRecord); err != nil {
			s.logger.Printf("insert grpc trx transfer out failed: tx=%s err=%v", txID, err)
		}
	}
	if okTo {
		inRecord := record
		inRecord.WatchAddress = toBase58
		if err := s.repo.InsertTransferIn(ctx, inRecord); err != nil {
			s.logger.Printf("insert grpc trx transfer in failed: tx=%s err=%v", txID, err)
		}
	}

	s.logger.Printf("grpc trx transfer matched: tx=%s block=%d from=%s to=%s amount=%s hit_from=%t hit_to=%t",
		txID, blockNum, record.FromAddress, record.ToAddress, record.Amount.String(), okFrom, okTo)

	if okFrom {
		s.refreshAddressBalances(ctx, fromBase58, txID, blockNum, "TRX", "OUT")
	}
	if okTo {
		s.refreshAddressBalances(ctx, toBase58, txID, blockNum, "TRX", "IN")
	}
}

func (s *TronGRPCBackupSync) handleUSDTTransfers(ctx context.Context, txID string, blockNum, blockTime int64) {
	info, err := s.client.GetTransactionInfoByID(ctx, txID, s.blockSource)
	if err != nil {
		s.logger.Printf("load grpc tx info failed: tx=%s err=%v", txID, err)
		return
	}

	for idx, logItem := range info.GetLog() {
		if !s.client.IsUSDTTransferLog(logItem) {
			continue
		}

		fromHex, toHex, amount, err := s.client.DecodeTransferLog(logItem)
		if err != nil {
			s.logger.Printf("decode grpc usdt transfer failed: tx=%s err=%v", txID, err)
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
				String: tron.NormalizeHexAddress(hex.EncodeToString(logItem.GetAddress())),
				Valid:  len(logItem.GetAddress()) > 0,
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
				s.logger.Printf("insert grpc usdt transfer out failed: tx=%s err=%v", txID, err)
			}
		}
		if okTo {
			inRecord := record
			inRecord.WatchAddress = toBase58
			if err := s.repo.InsertTransferIn(ctx, inRecord); err != nil {
				s.logger.Printf("insert grpc usdt transfer in failed: tx=%s err=%v", txID, err)
			}
		}

		s.logger.Printf("grpc usdt transfer matched: tx=%s block=%d from=%s to=%s amount=%s hit_from=%t hit_to=%t",
			txID, blockNum, record.FromAddress, record.ToAddress, record.Amount.String(), okFrom, okTo)

		if okFrom {
			s.refreshAddressBalances(ctx, fromBase58, txID, blockNum, "USDT", "OUT")
		}
		if okTo {
			s.refreshAddressBalances(ctx, toBase58, txID, blockNum, "USDT", "IN")
		}
	}
}

func (s *TronGRPCBackupSync) refreshAddressBalances(ctx context.Context, addressBase58, txID string, blockNum int64, triggerAsset, direction string) {
	if strings.TrimSpace(addressBase58) == "" {
		return
	}

	s.logger.Printf("transfer matched -> refresh balances: address=%s trigger_asset=%s direction=%s tx=%s block=%d refresh_assets=TRX,USDT source=onchain",
		addressBase58, triggerAsset, direction, txID, blockNum)

	active, trxBalance, err := s.client.GetAccountState(ctx, addressBase58)
	if err != nil {
		s.logger.Printf("grpc refresh trx balance failed: address=%s tx=%s err=%v", addressBase58, txID, err)
	} else if err := s.repo.UpsertBalance(ctx, addressBase58, "TRX", trxBalance, blockNum); err != nil {
		s.logger.Printf("grpc save trx balance failed: address=%s tx=%s err=%v", addressBase58, txID, err)
	} else {
		s.logger.Printf("balance updated: address=%s asset=TRX balance=%s block=%d inactive=%t source=onchain", addressBase58, trxBalance.String(), blockNum, !active)
	}

	usdtBalance, err := s.client.GetUSDTBalance(ctx, addressBase58)
	if err != nil {
		s.logger.Printf("grpc refresh usdt balance failed: address=%s tx=%s err=%v", addressBase58, txID, err)
		return
	}
	if err := s.repo.UpsertBalance(ctx, addressBase58, "USDT", usdtBalance, blockNum); err != nil {
		s.logger.Printf("grpc save usdt balance failed: address=%s tx=%s err=%v", addressBase58, txID, err)
		return
	}
	s.logger.Printf("balance updated: address=%s asset=USDT balance=%s block=%d source=onchain", addressBase58, usdtBalance.String(), blockNum)
}

func fallbackBase58FromBytes(addressBytes []byte, fallback string) string {
	if fallback != "" {
		return fallback
	}
	if len(addressBytes) == 0 {
		return ""
	}
	return gotronAddress.Address(addressBytes).String()
}
