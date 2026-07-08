package hdwallet

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSaveConfigEncryptsMnemonics(t *testing.T) {
	dir := t.TempDir()
	service := NewService(dir, 10000, nil, nil)

	cfg, err := service.SaveConfig(testMnemonic, testMnemonic, "10", "10")
	if err != nil {
		t.Fatalf("save config failed: %v", err)
	}
	if cfg.TronMnemonic != testMnemonic {
		t.Fatalf("expected plaintext response")
	}

	raw, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("read config file failed: %v", err)
	}
	if strings.Contains(string(raw), testMnemonic) {
		t.Fatalf("config.json should not contain plaintext mnemonic")
	}

	loaded, err := service.loadConfig()
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if loaded.TronMnemonic != testMnemonic || loaded.BSCMnemonic != testMnemonic {
		t.Fatalf("expected decrypted mnemonics after load")
	}
	if cfg.TronUSDTThreshold != "10" || cfg.BSCUSDTThreshold != "10" {
		t.Fatalf("expected threshold config to persist")
	}
}

func TestLoadConfigSupportsLegacyPlaintext(t *testing.T) {
	dir := t.TempDir()
	service := NewService(dir, 10000, nil, nil)

	legacy := ConfigFile{
		TronMnemonic: testMnemonic,
		BSCMnemonic:  testMnemonic,
		Count:        10000,
		UpdatedAt:    "2026-07-07 00:00:00",
	}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal legacy config failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), data, 0o600); err != nil {
		t.Fatalf("write legacy config failed: %v", err)
	}

	loaded, err := service.loadConfig()
	if err != nil {
		t.Fatalf("load legacy config failed: %v", err)
	}
	if loaded.TronMnemonic != testMnemonic || loaded.BSCMnemonic != testMnemonic {
		t.Fatalf("expected plaintext compatibility")
	}
	if loaded.TronUSDTThreshold != "10" || loaded.BSCUSDTThreshold != "10" {
		t.Fatalf("expected default thresholds for legacy config")
	}
}
