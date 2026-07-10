package service

import (
	"testing"

	"tron_watcher/internal/config"
)

func TestNormalizeActivatorPrivateKeysSupportsLegacyAndList(t *testing.T) {
	cfg := config.TronActivatorConfig{
		PrivateKey: " 0xaaa ",
		PrivateKeys: []string{
			"",
			"bbb",
			"0xaaa",
			" 0xccc ",
		},
	}

	got := normalizeActivatorPrivateKeys(cfg)
	want := []string{"aaa", "bbb", "ccc"}
	if len(got) != len(want) {
		t.Fatalf("expected %d keys, got %d: %#v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected key[%d]=%q, got %q", i, want[i], got[i])
		}
	}
}

func TestPickSignerRoundRobin(t *testing.T) {
	activator := &TronAddressActivator{
		signers: []tronActivatorSigner{
			{fromBase58: "A"},
			{fromBase58: "B"},
			{fromBase58: "C"},
		},
	}

	want := []string{"A", "B", "C", "A", "B"}
	for i, expected := range want {
		signer, err := activator.pickSigner()
		if err != nil {
			t.Fatalf("pickSigner call %d returned error: %v", i, err)
		}
		if signer.fromBase58 != expected {
			t.Fatalf("pickSigner call %d expected %q, got %q", i, expected, signer.fromBase58)
		}
	}
}

func TestPickSignerByRecordIDIsDeterministic(t *testing.T) {
	activator := &TronAddressActivator{
		signers: []tronActivatorSigner{
			{fromBase58: "A"},
			{fromBase58: "B"},
			{fromBase58: "C"},
		},
	}

	first, err := activator.pickSignerByRecordID(123456)
	if err != nil {
		t.Fatalf("first pickSignerByRecordID returned error: %v", err)
	}

	for i := 0; i < 5; i++ {
		got, err := activator.pickSignerByRecordID(123456)
		if err != nil {
			t.Fatalf("pickSignerByRecordID iteration %d returned error: %v", i, err)
		}
		if got.fromBase58 != first.fromBase58 {
			t.Fatalf("expected deterministic signer %q, got %q", first.fromBase58, got.fromBase58)
		}
	}
}

func TestSignerIndexByRecordIDIsDeterministic(t *testing.T) {
	activator := &TronAddressActivator{
		signers: []tronActivatorSigner{
			{fromBase58: "A"},
			{fromBase58: "B"},
			{fromBase58: "C"},
			{fromBase58: "D"},
		},
	}

	first, err := activator.SignerIndexByRecordID(999)
	if err != nil {
		t.Fatalf("SignerIndexByRecordID returned error: %v", err)
	}
	if first < 0 || first >= len(activator.signers) {
		t.Fatalf("signer index out of range: %d", first)
	}

	for i := 0; i < 5; i++ {
		got, err := activator.SignerIndexByRecordID(999)
		if err != nil {
			t.Fatalf("SignerIndexByRecordID iteration %d returned error: %v", i, err)
		}
		if got != first {
			t.Fatalf("expected deterministic signer index %d, got %d", first, got)
		}
	}
}

func TestSignerIndexByRecordIDFallsBackToFirstSignerWhenOnlyOneSigner(t *testing.T) {
	activator := &TronAddressActivator{
		signers: []tronActivatorSigner{
			{fromBase58: "A"},
		},
	}

	got, err := activator.SignerIndexByRecordID(999)
	if err != nil {
		t.Fatalf("SignerIndexByRecordID returned error: %v", err)
	}
	if got != 0 {
		t.Fatalf("expected fallback signer index 0, got %d", got)
	}
}
