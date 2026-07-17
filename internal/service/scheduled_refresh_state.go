package service

import (
	"sort"
	"strings"
	"sync"
	"time"
)

type ScheduledRefreshStateTracker struct {
	mu      sync.RWMutex
	running map[string]time.Time
}

func NewScheduledRefreshStateTracker() *ScheduledRefreshStateTracker {
	return &ScheduledRefreshStateTracker{
		running: make(map[string]time.Time),
	}
}

func (t *ScheduledRefreshStateTracker) Start(chain string) {
	if t == nil {
		return
	}
	chain = normalizeRefreshChain(chain)
	if chain == "" {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.running[chain] = time.Now()
}

func (t *ScheduledRefreshStateTracker) TryStart(chain string) (bool, []string) {
	if t == nil {
		return true, nil
	}
	chain = normalizeRefreshChain(chain)
	if chain == "" {
		return false, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.running) > 0 {
		chains := make([]string, 0, len(t.running))
		for item := range t.running {
			chains = append(chains, item)
		}
		sort.Strings(chains)
		return false, chains
	}

	t.running[chain] = time.Now()
	return true, nil
}

func (t *ScheduledRefreshStateTracker) Finish(chain string) {
	if t == nil {
		return
	}
	chain = normalizeRefreshChain(chain)
	if chain == "" {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.running, chain)
}

func (t *ScheduledRefreshStateTracker) ActiveChains() []string {
	if t == nil {
		return nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	chains := make([]string, 0, len(t.running))
	for chain := range t.running {
		chains = append(chains, chain)
	}
	sort.Strings(chains)
	return chains
}

func normalizeRefreshChain(chain string) string {
	return strings.ToLower(strings.TrimSpace(chain))
}
