package service

import (
	"context"
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
	blockNumber, err := s.tronClient.GetSolidBlockNumber(ctx)
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

		s.refreshBalance(ctx, tronBalanceTask{
			addressBase58: addressBase58,
			addressHex:    addressHex,
			asset:         "TRX",
		}, blockNumber)
		if perCallDelay > 0 {
			timer := time.NewTimer(perCallDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}

		s.refreshBalance(ctx, tronBalanceTask{
			addressBase58: addressBase58,
			addressHex:    addressHex,
			asset:         "USDT",
		}, blockNumber)
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
		active, _, err := s.tronClient.GetAccountState(ctx, task.addressHex)
		if err != nil {
			s.logger.Printf("refresh tron account state failed: %s err=%v", task.addressBase58, err)
			return
		}
		if !active {
			if err := s.repo.UpsertBalance(ctx, task.addressBase58, "USDT", decimal.Zero, blockNumber); err != nil {
				s.logger.Printf("save usdt balance failed: %s err=%v", task.addressBase58, err)
				return
			}
			s.logger.Printf("skip usdt balance refresh for inactive tron address: address=%s block=%d", task.addressBase58, blockNumber)
			return
		}
		balance, err := s.tronClient.GetUSDTBalance(ctx, task.addressHex)
		if err != nil {
			s.logger.Printf("refresh usdt balance failed: %s err=%v", task.addressBase58, err)
			return
		}
		if err := s.repo.UpsertBalance(ctx, task.addressBase58, "USDT", balance, blockNumber); err != nil {
			s.logger.Printf("save usdt balance failed: %s err=%v", task.addressBase58, err)
			return
		}
		s.logger.Printf("balance updated: address=%s asset=USDT balance=%s block=%d", task.addressBase58, balance.String(), blockNumber)
	}
}
