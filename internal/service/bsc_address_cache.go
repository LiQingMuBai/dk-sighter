package service

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"tron_watcher/internal/repository"
)

type BSCAddressCache struct {
	repo   *repository.DB
	logger *log.Logger
	source string

	mu    sync.RWMutex
	addrs map[string]struct{}
}

func NewBSCAddressCache(repo *repository.DB) *BSCAddressCache {
	return &BSCAddressCache{
		repo:   repo,
		logger: bscLogger(),
		addrs:  make(map[string]struct{}),
	}
}

func (c *BSCAddressCache) SetLogger(logger *log.Logger) {
	if c == nil || logger == nil {
		return
	}
	c.logger = logger
}

func (c *BSCAddressCache) ConfigureSource(source string) {
	c.source = strings.TrimSpace(source)
}

func (c *BSCAddressCache) Run(ctx context.Context, interval time.Duration) error {
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
				c.logger.Printf("reload bsc addresses failed: %v", err)
			}
		}
	}
}

func (c *BSCAddressCache) Reload(ctx context.Context) error {
	var (
		items []string
		err   error
	)
	if strings.TrimSpace(c.source) != "" {
		items, err = repository.LoadActiveBSCWatchAddressesBySource(ctx, c.repo, c.source)
	} else {
		items, err = repository.LoadActiveBSCWatchAddresses(ctx, c.repo)
	}
	if err != nil {
		return err
	}

	next := make(map[string]struct{}, len(items))
	for _, item := range items {
		addr := strings.ToLower(strings.TrimSpace(item))
		if addr == "" {
			continue
		}
		next[addr] = struct{}{}
	}

	c.mu.Lock()
	c.addrs = next
	c.mu.Unlock()

	c.logger.Printf("bsc address cache reloaded: %d active addresses", len(next))
	return nil
}

func (c *BSCAddressCache) Has(address string) bool {
	address = strings.ToLower(strings.TrimSpace(address))
	if address == "" {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.addrs[address]
	return ok
}

func (c *BSCAddressCache) List() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]string, 0, len(c.addrs))
	for addr := range c.addrs {
		result = append(result, addr)
	}
	return result
}
