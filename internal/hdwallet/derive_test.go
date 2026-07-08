package hdwallet

import (
	"strings"
	"testing"

	"tron_watcher/internal/tron"
)

const testMnemonic = "test test test test test test test test test test test junk"

func TestDeriveBSCAddresses(t *testing.T) {
	records, err := DeriveBSCAddresses(testMnemonic, 1)
	if err != nil {
		t.Fatalf("derive bsc addresses failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Address != "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266" {
		t.Fatalf("unexpected first bsc address: %s", records[0].Address)
	}
}

func TestDeriveTronAddresses(t *testing.T) {
	records, err := DeriveTronAddresses(testMnemonic, 2)
	if err != nil {
		t.Fatalf("derive tron addresses failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0].Address == records[1].Address {
		t.Fatalf("expected distinct addresses")
	}
	for _, record := range records {
		if !strings.HasPrefix(record.AddressHex, "41") {
			t.Fatalf("expected tron hex prefix 41, got %s", record.AddressHex)
		}
		decoded, err := tron.Base58ToHex(record.Address)
		if err != nil {
			t.Fatalf("decode tron address failed: %v", err)
		}
		if decoded != record.AddressHex {
			t.Fatalf("expected hex %s, got %s", record.AddressHex, decoded)
		}
	}
}
