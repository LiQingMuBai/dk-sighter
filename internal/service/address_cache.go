package service

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"tron_watcher/internal/repository"
	"tron_watcher/internal/tron"
)

type AddressCache struct {
	repo   *repository.DB
	logger *log.Logger
	source string

	mu       sync.RWMutex
	base58   map[string]struct{}
	hexToB58 map[string]string
}

func NewAddressCache(repo *repository.DB) *AddressCache {
	return &AddressCache{
		repo:     repo,
		logger:   tronLogger(),
		base58:   make(map[string]struct{}),
		hexToB58: make(map[string]string),
	}
}

func (c *AddressCache) ConfigureSource(source string) {
	c.source = strings.TrimSpace(source)
}

func (c *AddressCache) Run(ctx context.Context, interval time.Duration) error {
	if err := c.Reload(ctx); err != nil {
		return err
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.Reload(ctx); err != nil {
				log.Printf("reload addresses failed: %v", err)
			}
		}
	}
}

func (c *AddressCache) Reload(ctx context.Context) error {
	var (
		items []repository.WatchAddress
		err   error
	)
	if strings.TrimSpace(c.source) != "" {
		items, err = c.repo.LoadActiveAddressesBySource(ctx, c.source)
	} else {
		items, err = c.repo.LoadActiveAddresses(ctx)
	}
	if err != nil {
		return err
	}

	nextBase58 := make(map[string]struct{}, len(items))
	nextHexToB58 := make(map[string]string, len(items))

	for _, item := range items {
		base58 := strings.TrimSpace(item.AddressBase58)
		hexAddr := ""
		if base58 != "" {
			if parsed, convErr := tron.Base58ToHex(base58); convErr == nil {
				hexAddr = parsed
			}
		}

		if base58 == "" || hexAddr == "" {
			continue
		}

		nextBase58[base58] = struct{}{}
		nextHexToB58[hexAddr] = base58
	}

	c.mu.Lock()
	c.base58 = nextBase58
	c.hexToB58 = nextHexToB58
	c.mu.Unlock()

	c.logger.Printf("address cache reloaded: %d active addresses", len(nextBase58))
	return nil
}

func (c *AddressCache) HasBase58(address string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.base58[address]
	return ok
}

func (c *AddressCache) Base58ByHex(hexAddr string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, ok := c.hexToB58[tron.NormalizeHexAddress(hexAddr)]
	return value, ok
}

func (c *AddressCache) List() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]string, 0, len(c.base58))
	for addr := range c.base58 {
		result = append(result, addr)
	}
	return result
}
