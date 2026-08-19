package ushield_trace

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"tron_watcher/internal/config"
	"tron_watcher/internal/database"
)

type UserTrace struct {
	Network string
	Address string
	ChatID  string
}

type Service struct {
	db       *sql.DB
	network  string
	logger   *log.Logger
	enabled  bool

	reloadInterval time.Duration

	mu     sync.RWMutex
	byAddr map[string][]string
}

func NewService(cfg config.MySQLConfig, network string) (*Service, error) {
	network = strings.ToLower(strings.TrimSpace(network))
	if network == "" {
		network = "bsc"
	}

	dsn := strings.TrimSpace(cfg.DSN)
	if dsn == "" {
		return &Service{
			network: network,
			logger:  log.New(log.Writer(), "[ushield-trace] ", log.LstdFlags),
			enabled: false,
		}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := database.NewMySQL(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect ushield mysql: %w", err)
	}

	return &Service{
		db:             db,
		network:        network,
		logger:         log.New(log.Writer(), "[ushield-trace] ", log.LstdFlags),
		enabled:        true,
		reloadInterval: 15 * time.Second,
		byAddr:         make(map[string][]string),
	}, nil
}

func (s *Service) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Service) Enabled() bool {
	return s != nil && s.enabled
}

func (s *Service) SetLogger(logger *log.Logger) {
	if s == nil || logger == nil {
		return
	}
	s.logger = logger
}

func (s *Service) SetReloadInterval(d time.Duration) {
	if s == nil {
		return
	}
	if d <= 0 {
		d = 15 * time.Second
	}
	s.reloadInterval = d
}

func (s *Service) Run(ctx context.Context) error {
	if !s.Enabled() {
		s.logger.Printf("ushield_trace disabled: ushield_mysql.dsn is empty")
		<-ctx.Done()
		return nil
	}

	if err := s.Reload(ctx); err != nil {
		s.logger.Printf("initial reload failed: %v", err)
	}

	ticker := time.NewTicker(s.reloadInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.Reload(ctx); err != nil {
				s.logger.Printf("reload failed: %v", err)
			}
		}
	}
}

func (s *Service) Reload(ctx context.Context) error {
	if !s.Enabled() {
		return nil
	}

	q := `
		SELECT LOWER(TRIM(address)), TRIM(chat_id)
		FROM user_address_trace
		WHERE network = ?
		  AND address IS NOT NULL
		  AND chat_id IS NOT NULL
		  AND LENGTH(TRIM(address)) > 0
		  AND LENGTH(TRIM(chat_id)) > 0
		ORDER BY id ASC
	`
	rows, err := s.db.QueryContext(ctx, q, s.network)
	if err != nil {
		return fmt.Errorf("query user_address_trace: %w", err)
	}
	defer rows.Close()

	next := make(map[string][]string)
	uniq := make(map[string]map[string]struct{})
	var (
		addr   string
		chatID string
	)
	totalRows := 0
	for rows.Next() {
		totalRows++
		if err := rows.Scan(&addr, &chatID); err != nil {
			return fmt.Errorf("scan user_address_trace: %w", err)
		}
		addr = strings.ToLower(strings.TrimSpace(addr))
		chatID = strings.TrimSpace(chatID)
		if addr == "" || chatID == "" {
			continue
		}
		if _, ok := uniq[addr]; !ok {
			uniq[addr] = make(map[string]struct{})
		}
		if _, ok := uniq[addr][chatID]; ok {
			continue
		}
		uniq[addr][chatID] = struct{}{}
		next[addr] = append(next[addr], chatID)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows err: %w", err)
	}

	s.mu.Lock()
	old := len(s.byAddr)
	s.byAddr = next
	s.mu.Unlock()

	s.logger.Printf("cache reloaded: network=%s old_addrs=%d new_addrs=%d total_rows=%d", s.network, old, len(next), totalRows)
	return nil
}

func (s *Service) FindChatID(address string) (string, bool) {
	if !s.Enabled() {
		return "", false
	}
	address = strings.ToLower(strings.TrimSpace(address))
	if address == "" {
		return "", false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	list, ok := s.byAddr[address]
	if !ok || len(list) == 0 {
		return "", false
	}
	return list[len(list)-1], true
}

func (s *Service) FindChatIDs(address string) ([]string, bool) {
	if !s.Enabled() {
		return nil, false
	}
	address = strings.ToLower(strings.TrimSpace(address))
	if address == "" {
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	list, ok := s.byAddr[address]
	if !ok || len(list) == 0 {
		return nil, false
	}
	out := make([]string, len(list))
	copy(out, list)
	return out, true
}

func placeholders(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('?')
	}
	return b.String()
}

func (s *Service) FindChatIDsDirect(addresses []string) (map[string][]string, error) {
	result := make(map[string][]string)
	if !s.Enabled() {
		return result, nil
	}
	if len(addresses) == 0 {
		return result, nil
	}
	lowerAddrs := make([]string, 0, len(addresses))
	seen := make(map[string]struct{})
	for _, a := range addresses {
		k := strings.ToLower(strings.TrimSpace(a))
		if k == "" {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		lowerAddrs = append(lowerAddrs, k)
	}
	if len(lowerAddrs) == 0 {
		return result, nil
	}
	q := fmt.Sprintf(`
		SELECT LOWER(TRIM(address)), TRIM(chat_id)
		FROM user_address_trace
		WHERE network = ? AND LOWER(TRIM(address)) IN (%s)
		  AND address IS NOT NULL
		  AND chat_id IS NOT NULL
		  AND LENGTH(TRIM(address)) > 0
		  AND LENGTH(TRIM(chat_id)) > 0
		ORDER BY id ASC
	`, placeholders(len(lowerAddrs)))
	args := make([]interface{}, 0, 1+len(lowerAddrs))
	args = append(args, s.network)
	for _, a := range lowerAddrs {
		args = append(args, a)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	uniq := make(map[string]map[string]struct{})
	for rows.Next() {
		var addr, chatID string
		if err := rows.Scan(&addr, &chatID); err != nil {
			continue
		}
		addr = strings.ToLower(strings.TrimSpace(addr))
		chatID = strings.TrimSpace(chatID)
		if addr == "" || chatID == "" {
			continue
		}
		if _, ok := uniq[addr]; !ok {
			uniq[addr] = make(map[string]struct{})
		}
		if _, ok := uniq[addr][chatID]; ok {
			continue
		}
		uniq[addr][chatID] = struct{}{}
		result[addr] = append(result[addr], chatID)
	}
	return result, nil
}
