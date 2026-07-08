package web

import (
	"strings"
	"sync"
	"time"
)

type mnemonicStore struct {
	mu        sync.Mutex
	value     string
	expiresAt time.Time
}

func newMnemonicStore() *mnemonicStore {
	return &mnemonicStore{}
}

func (s *mnemonicStore) Set(value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.value = strings.TrimSpace(value)
	s.expiresAt = time.Now().Add(ttl)
}

func (s *mnemonicStore) Get() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.value == "" || time.Now().After(s.expiresAt) {
		s.value = ""
		s.expiresAt = time.Time{}
		return "", false
	}
	return s.value, true
}

func normalizeMnemonic(input string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(input)), " ")
}

func isValidMnemonic(input string) bool {
	words := strings.Fields(strings.TrimSpace(input))
	return len(words) == 12 || len(words) == 24
}
