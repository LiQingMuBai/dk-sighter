package hdwallet

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"

	"tron_watcher/internal/tron"
)

type blockSyncState struct {
	tronMu sync.Mutex
	bscMu  sync.Mutex
}

var hdBlockSyncState blockSyncState

func (s *Service) RunTronBlockSync(ctx context.Context, interval time.Duration, startBlock int64) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := s.scanTronBlocks(ctx, startBlock); err != nil && err != context.Canceled {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *Service) RunBSCBlockSync(ctx context.Context, interval time.Duration, startBlock int64, confirmations int) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := s.scanBSCBlocks(ctx, startBlock, confirmations); err != nil && err != context.Canceled {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *Service) scanTronBlocks(ctx context.Context, startBlock int64) error {
	if s.tronClient == nil {
		return nil
	}

	hdBlockSyncState.tronMu.Lock()
	defer hdBlockSyncState.tronMu.Unlock()
	s.setChainSyncRunning("tron", true)
	defer s.setChainSyncRunning("tron", false)

	cfg, err := s.loadConfig()
	if err != nil {
		return err
	}
	if strings.TrimSpace(cfg.TronMnemonic) == "" {
		return nil
	}

	file, err := s.ensureTronFile(cfg)
	if err != nil {
		return err
	}

	var headBlock int64
	var latestSolid int64
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
	file.LatestChainBlock = headBlock

	if file.LastScannedBlock == 0 {
		initialBlock := latestSolid
		if startBlock > 0 && startBlock < initialBlock {
			initialBlock = startBlock
		}
		file.LastScannedBlock = initialBlock
		file.LastScheduledSyncAt = nowString()
		return s.writeJSON(s.chainPath("tron"), file)
	}

	if file.LastScannedBlock >= latestSolid {
		return nil
	}

	index := buildTronAddressIndex(file)
	for blockNum := file.LastScannedBlock + 1; blockNum <= latestSolid; blockNum++ {
		if err := s.scanSingleTronBlock(ctx, file, index, blockNum); err != nil {
			return err
		}
		file.LastScannedBlock = blockNum
		file.LastScheduledSyncAt = nowString()
		if err := s.writeJSON(s.chainPath("tron"), file); err != nil {
			return err
		}
	}

	_ = headBlock
	return nil
}

func (s *Service) scanBSCBlocks(ctx context.Context, startBlock int64, confirmations int) error {
	if s.bscClient == nil {
		return nil
	}

	hdBlockSyncState.bscMu.Lock()
	defer hdBlockSyncState.bscMu.Unlock()
	s.setChainSyncRunning("bsc", true)
	defer s.setChainSyncRunning("bsc", false)

	cfg, err := s.loadConfig()
	if err != nil {
		return err
	}
	if strings.TrimSpace(cfg.BSCMnemonic) == "" {
		return nil
	}

	file, err := s.ensureBSCFile(cfg)
	if err != nil {
		return err
	}

	latestBlock, err := s.bscClient.BlockNumber(ctx)
	if err != nil {
		return err
	}
	latest := int64(latestBlock)
	if confirmations > 0 && latest > int64(confirmations) {
		latest -= int64(confirmations)
	}
	file.LatestChainBlock = latest

	if file.LastScannedBlock == 0 {
		initialBlock := latest
		if startBlock > 0 && startBlock < initialBlock {
			initialBlock = startBlock
		}
		file.LastScannedBlock = initialBlock
		file.LastScheduledSyncAt = nowString()
		return s.writeJSON(s.chainPath("bsc"), file)
	}

	if file.LastScannedBlock >= latest {
		return nil
	}

	index := buildBSCAddressIndex(file)
	for blockNum := file.LastScannedBlock + 1; blockNum <= latest; blockNum++ {
		if err := s.scanSingleBSCBlock(ctx, file, index, uint64(blockNum)); err != nil {
			return err
		}
		file.LastScannedBlock = blockNum
		file.LastScheduledSyncAt = nowString()
		if err := s.writeJSON(s.chainPath("bsc"), file); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) scanSingleTronBlock(ctx context.Context, file *ChainFile, index map[string]int, blockNum int64) error {
	block, err := s.tronClient.GetBlockByNum(ctx, blockNum)
	if err != nil {
		return err
	}

	touched := make(map[int]struct{})
	for _, tx := range block.Transactions {
		if len(tx.RawData.Contract) == 0 {
			continue
		}

		contract := tx.RawData.Contract[0]
		if contract.Type == "TransferContract" {
			fromHex := tron.NormalizeHexAddress(contract.Parameter.Value.OwnerAddress)
			toHex := tron.NormalizeHexAddress(contract.Parameter.Value.ToAddress)
			if idx, ok := index[fromHex]; ok {
				touched[idx] = struct{}{}
			}
			if idx, ok := index[toHex]; ok {
				touched[idx] = struct{}{}
			}
		}

		if contract.Type != "TriggerSmartContract" {
			continue
		}
		info, err := s.tronClient.GetTransactionInfoByID(ctx, tx.TxID)
		if err != nil {
			return err
		}
		for _, logItem := range info.Log {
			if !s.tronClient.IsUSDTTransferLog(logItem) {
				continue
			}
			fromHex, toHex, _, err := s.tronClient.DecodeTransferLog(logItem)
			if err != nil {
				continue
			}
			if idx, ok := index[tron.NormalizeHexAddress(fromHex)]; ok {
				touched[idx] = struct{}{}
			}
			if idx, ok := index[tron.NormalizeHexAddress(toHex)]; ok {
				touched[idx] = struct{}{}
			}
		}
	}

	return s.refreshTronTouchedBalances(ctx, file, touched)
}

func (s *Service) scanSingleBSCBlock(ctx context.Context, file *ChainFile, index map[string]int, blockNum uint64) error {
	var (
		block         TouchedBSCBlock
		usdtTransfers []TouchedBSCTransfer
	)
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		value, err := s.bscClient.GetBlockByNumber(groupCtx, blockNum)
		if err != nil {
			return err
		}
		block.Transactions = make([]TouchedBSCTransaction, 0, len(value.Transactions))
		for _, item := range value.Transactions {
			block.Transactions = append(block.Transactions, TouchedBSCTransaction{
				From: item.From,
				To:   item.To,
			})
		}
		return nil
	})
	group.Go(func() error {
		value, err := s.bscClient.GetUSDTTransfersByBlock(groupCtx, blockNum)
		if err != nil {
			return err
		}
		usdtTransfers = make([]TouchedBSCTransfer, 0, len(value))
		for _, item := range value {
			usdtTransfers = append(usdtTransfers, TouchedBSCTransfer{From: item.From, To: item.To})
		}
		return nil
	})
	if err := group.Wait(); err != nil {
		return err
	}

	touched := make(map[int]struct{})
	for _, tx := range block.Transactions {
		if idx, ok := index[strings.ToLower(strings.TrimSpace(tx.From))]; ok {
			touched[idx] = struct{}{}
		}
		if idx, ok := index[strings.ToLower(strings.TrimSpace(tx.To))]; ok {
			touched[idx] = struct{}{}
		}
	}
	for _, transfer := range usdtTransfers {
		if idx, ok := index[strings.ToLower(strings.TrimSpace(transfer.From))]; ok {
			touched[idx] = struct{}{}
		}
		if idx, ok := index[strings.ToLower(strings.TrimSpace(transfer.To))]; ok {
			touched[idx] = struct{}{}
		}
	}

	return s.refreshBSCTouchedBalances(ctx, file, touched)
}

func (s *Service) refreshTronTouchedBalances(ctx context.Context, file *ChainFile, touched map[int]struct{}) error {
	if len(touched) == 0 {
		return nil
	}

	indexes := sortTouchedIndexes(touched)
	for _, idx := range indexes {
		record := &file.Addresses[idx]
		active, trxBalance, err := s.tronClient.GetAccountState(ctx, record.AddressHex)
		if err != nil {
			return fmt.Errorf("读取 tron 地址 %s trx 余额失败: %w", record.Address, err)
		}
		if err := s.waitForBalanceThrottle(ctx); err != nil {
			return err
		}
		usdtBalance := decimal.Zero
		if active {
			usdtBalance, err = s.tronClient.GetUSDTBalance(ctx, record.AddressHex)
			if err != nil {
				return fmt.Errorf("读取 tron 地址 %s usdt 余额失败: %w", record.Address, err)
			}
			if err := s.waitForBalanceThrottle(ctx); err != nil {
				return err
			}
		}
		record.TRXBalance = trxBalance.StringFixed(6)
		record.USDTBalance = usdtBalance.StringFixed(6)
		record.UpdatedAt = nowString()
	}
	file.BalanceUpdatedAt = nowString()
	return nil
}

func (s *Service) refreshBSCTouchedBalances(ctx context.Context, file *ChainFile, touched map[int]struct{}) error {
	if len(touched) == 0 {
		return nil
	}

	indexes := sortTouchedIndexes(touched)
	for _, idx := range indexes {
		record := &file.Addresses[idx]
		bnbBalance, err := s.bscClient.GetBNBBalance(ctx, record.Address)
		if err != nil {
			return fmt.Errorf("读取 bsc 地址 %s bnb 余额失败: %w", record.Address, err)
		}
		if err := s.waitForBalanceThrottle(ctx); err != nil {
			return err
		}
		usdtBalance, err := s.bscClient.GetUSDTBalance(ctx, record.Address)
		if err != nil {
			return fmt.Errorf("读取 bsc 地址 %s usdt 余额失败: %w", record.Address, err)
		}
		if err := s.waitForBalanceThrottle(ctx); err != nil {
			return err
		}
		record.BNBBalance = bnbBalance.StringFixed(6)
		record.USDTBalance = usdtBalance.StringFixed(6)
		record.UpdatedAt = nowString()
	}
	file.BalanceUpdatedAt = nowString()
	return nil
}

func buildTronAddressIndex(file *ChainFile) map[string]int {
	index := make(map[string]int, len(file.Addresses))
	for i, item := range file.Addresses {
		if item.AddressHex != "" {
			index[tron.NormalizeHexAddress(item.AddressHex)] = i
		}
	}
	return index
}

func buildBSCAddressIndex(file *ChainFile) map[string]int {
	index := make(map[string]int, len(file.Addresses))
	for i, item := range file.Addresses {
		address := strings.ToLower(strings.TrimSpace(item.Address))
		if address != "" {
			index[address] = i
		}
	}
	return index
}

func sortTouchedIndexes(touched map[int]struct{}) []int {
	indexes := make([]int, 0, len(touched))
	for idx := range touched {
		indexes = append(indexes, idx)
	}
	sort.Ints(indexes)
	return indexes
}

type TouchedBSCBlock struct {
	Transactions []TouchedBSCTransaction
}

type TouchedBSCTransaction struct {
	From string
	To   string
}

type TouchedBSCTransfer struct {
	From string
	To   string
}
