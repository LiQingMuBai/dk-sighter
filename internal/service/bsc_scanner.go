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
const bscBalanceWorkers = 6

type BSCScanner struct {
	client        *bsc.Client
	repo          *repository.DB
	cache         *BSCAddressCache
	notifier      TransferNotifier
	startBlock    int64
	confirmations int
	logger        *log.Logger

	triggerCh chan struct{}
	runMu     sync.Mutex
}

func NewBSCScanner(client *bsc.Client, repo *repository.DB, cache *BSCAddressCache, notifier TransferNotifier, startBlock int64, confirmations int) *BSCScanner {
	if confirmations < 0 {
		confirmations = 0
	}
	return &BSCScanner{
		client:        client,
		repo:          repo,
		cache:         cache,
		notifier:      notifier,
		startBlock:    startBlock,
		confirmations: confirmations,
		logger:        bscLogger(),
		triggerCh:     make(chan struct{}, 1),
	}
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
			_ = repository.UpsertBSCBalance(ctx, s.repo, address, "USDT", normalizeDecimal(usdt))
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

func (s *BSCScanner) scan(ctx context.Context) error {
	s.runMu.Lock()
	defer s.runMu.Unlock()

	lastBlock, exists, err := s.repo.GetLastBlock(ctx, bscSyncKey)
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
		if err := s.repo.SaveLastBlock(ctx, bscSyncKey, initial); err != nil {
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

	scanLag := latestInt - lastBlock
	if scanLag < 0 {
		scanLag = 0
	}
	s.logger.Printf("scanner state: head=%d db_last=%d scan_lag=%d", latestInt, lastBlock, scanLag)
	if shouldSkipToLatestBlock(lastBlock, latestInt) {
		s.logger.Printf("scanner lag too large, skip to latest block: db_last=%d latest=%d lag=%d threshold=%d", lastBlock, latestInt, latestInt-lastBlock, maxAllowedSyncLagBlocks)
		if err := s.repo.SaveLastBlock(ctx, bscSyncKey, latestInt); err != nil {
			return err
		}
		return nil
	}
	if lastBlock < latestInt {
		s.logger.Printf("scanner catching up: from=%d to=%d", lastBlock+1, latestInt)
	}

	currentBlock := lastBlock
	for currentBlock < latestInt {
		latestDBBlock, changed, err := resolveSyncCursor(ctx, currentBlock, func(runCtx context.Context) (int64, bool, error) {
			return s.repo.GetLastBlock(runCtx, bscSyncKey)
		})
		if err != nil {
			return err
		}
		if changed {
			s.logger.Printf("scanner sync cursor updated from mysql: old=%d new=%d", currentBlock, latestDBBlock)
			currentBlock = latestDBBlock
			if shouldSkipToLatestBlock(currentBlock, latestInt) {
				s.logger.Printf("scanner lag too large after mysql cursor update, skip to latest block: db_last=%d latest=%d lag=%d threshold=%d", currentBlock, latestInt, latestInt-currentBlock, maxAllowedSyncLagBlocks)
				if err := s.repo.SaveLastBlock(ctx, bscSyncKey, latestInt); err != nil {
					return err
				}
				break
			}
			if currentBlock >= latestInt {
				break
			}
		}

		blockNum := currentBlock + 1
		if err := s.scanBlock(ctx, uint64(blockNum)); err != nil {
			return err
		}
		if err := s.repo.SaveLastBlock(ctx, bscSyncKey, blockNum); err != nil {
			return err
		}
		currentBlock = blockNum
	}

	return nil
}

func (s *BSCScanner) scanBlock(ctx context.Context, blockNum uint64) error {
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
		return err
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
		return nil
	}

	addrs := make([]string, 0, len(hit))
	for addr := range hit {
		addrs = append(addrs, addr)
	}

	s.logger.Printf("block matched: number=%d addresses=%d", blockNum, len(addrs))
	s.refreshBalances(ctx, addrs, false)
	return nil
}

func (s *BSCScanner) insertTransferIn(ctx context.Context, record repository.TransferRecord) {
	if err := s.repo.InsertBSCTransferIn(ctx, record); err != nil {
		s.logger.Printf("insert transfer in failed: tx=%s asset=%s err=%v", record.TxHash, record.AssetCode, err)
		return
	}
	if s.notifier != nil {
		s.notifier.NotifyTransfer(ctx, "bsc", "IN", record)
	}
}

func (s *BSCScanner) insertTransferOut(ctx context.Context, record repository.TransferRecord) {
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
	bnb, err := s.client.GetBNBBalance(ctx, address)
	if err != nil {
		s.logger.Printf("refresh bnb balance failed: %s err=%v", address, err)
	} else {
		_ = repository.UpsertBSCBalance(ctx, s.repo, address, "BNB", normalizeDecimal(bnb))
	}

	usdt, err := s.client.GetUSDTBalance(ctx, address)
	if err != nil {
		s.logger.Printf("refresh usdt balance failed: %s err=%v", address, err)
	} else {
		_ = repository.UpsertBSCBalance(ctx, s.repo, address, "USDT", normalizeDecimal(usdt))
	}
}

func normalizeDecimal(v decimal.Decimal) string {
	return v.String()
}
