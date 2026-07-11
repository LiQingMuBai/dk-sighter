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

const tronBalanceWorkers = 4

var usdtTransferRepairThreshold = decimal.NewFromInt(1)

type tronBalanceTask struct {
	addressBase58 string
	addressHex    string
	asset         string
}

type BalanceService struct {
	tronClient *tron.Client
	repo       *repository.DB
	cache      *AddressCache
	logger     *log.Logger

	mu    sync.Mutex
	dirty map[string]map[string]struct{}
}

func NewBalanceService(tronClient *tron.Client, repo *repository.DB, cache *AddressCache) *BalanceService {
	return &BalanceService{
		tronClient: tronClient,
		repo:       repo,
		cache:      cache,
		logger:     tronLogger(),
		dirty:      make(map[string]map[string]struct{}),
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
			s.logger.Printf("balance updated: address=%s asset=TRX balance=%s block=%d", addressBase58, trxBalance.String(), blockNumber)
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
				s.logger.Printf("balance updated: address=%s asset=USDT balance=%s block=%d", addressBase58, usdtBalance.String(), blockNumber)
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
	switch task.asset {
	case "TRX":
		active, balance, err := s.tronClient.GetAccountState(ctx, task.addressHex)
		if err != nil {
			s.logger.Printf("refresh trx balance failed: %s err=%v", task.addressBase58, err)
			return
		}
		if err := s.repo.UpsertBalance(ctx, task.addressBase58, "TRX", balance, blockNumber); err != nil {
			s.logger.Printf("save trx balance failed: %s err=%v", task.addressBase58, err)
			return
		}
		if !active {
			s.logger.Printf("balance updated: address=%s asset=TRX balance=%s block=%d inactive=true", task.addressBase58, balance.String(), blockNumber)
			return
		}
		s.logger.Printf("balance updated: address=%s asset=TRX balance=%s block=%d", task.addressBase58, balance.String(), blockNumber)
	case "USDT":
		balance, err := s.tronClient.GetUSDTBalance(ctx, task.addressHex)
		if err != nil {
			s.logger.Printf("refresh usdt balance failed: %s err=%v", task.addressBase58, err)
			return
		}
		currentDBBalance, balanceErr := s.getCurrentUSDTBalance(ctx, task.addressBase58)
		if balanceErr != nil {
			s.logger.Printf("load current usdt balance failed: %s err=%v", task.addressBase58, balanceErr)
		}
		if err := s.repo.UpsertBalance(ctx, task.addressBase58, "USDT", balance, blockNumber); err != nil {
			s.logger.Printf("save usdt balance failed: %s err=%v", task.addressBase58, err)
			return
		}
		s.logger.Printf("balance updated: address=%s asset=USDT balance=%s block=%d", task.addressBase58, balance.String(), blockNumber)
		if balanceErr == nil {
			s.syncRecentUSDTTransfersIfNeeded(ctx, task.addressBase58, task.addressHex, currentDBBalance, balance)
		}
	}
}

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

	insertedIn, insertedOut, err := s.syncRecentTronUSDTTransfers(ctx, addressBase58, addressHex, time.Now().Add(-time.Hour))
	if err != nil {
		s.logger.Printf("repair tron usdt transfers failed: address=%s old_balance=%s new_balance=%s err=%v", addressBase58, currentDBBalance.String(), latestBalance.String(), err)
		return
	}
	s.logger.Printf("repair tron usdt transfers done: address=%s old_balance=%s new_balance=%s inserted_in=%d inserted_out=%d",
		addressBase58, currentDBBalance.String(), latestBalance.String(), insertedIn, insertedOut)
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
			if !isTriggerSmartContractTx(tx) {
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
