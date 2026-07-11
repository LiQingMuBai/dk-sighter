package hdwallet

import (
	"encoding/json"
	"testing"

	"github.com/shopspring/decimal"
)

func TestBuildSignedTronTransactionJSON(t *testing.T) {
	unsignedJSON := []byte(`{
		"txID":"abc123",
		"raw_data":{
			"contract":[
				{
					"parameter":{
						"value":{"amount":1},
						"type_url":"type.googleapis.com/protocol.TriggerSmartContract"
					}
				}
			]
		},
		"raw_data_hex":"deadbeef"
	}`)

	signedJSON, err := buildSignedTronTransactionJSON(unsignedJSON, [][]byte{{0xaa, 0xbb, 0xcc}})
	if err != nil {
		t.Fatalf("buildSignedTronTransactionJSON returned error: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(signedJSON, &payload); err != nil {
		t.Fatalf("unmarshal signed json: %v", err)
	}

	signatures, ok := payload["signature"].([]any)
	if !ok || len(signatures) != 1 || signatures[0] != "aabbcc" {
		t.Fatalf("unexpected signature payload: %#v", payload["signature"])
	}

	rawData, ok := payload["raw_data"].(map[string]any)
	if !ok {
		t.Fatalf("raw_data missing or invalid: %#v", payload["raw_data"])
	}
	contracts, ok := rawData["contract"].([]any)
	if !ok || len(contracts) != 1 {
		t.Fatalf("contract missing or invalid: %#v", rawData["contract"])
	}
	contract, ok := contracts[0].(map[string]any)
	if !ok {
		t.Fatalf("contract item invalid: %#v", contracts[0])
	}
	parameter, ok := contract["parameter"].(map[string]any)
	if !ok {
		t.Fatalf("parameter invalid: %#v", contract["parameter"])
	}
	if parameter["type_url"] != "type.googleapis.com/protocol.TriggerSmartContract" {
		t.Fatalf("type_url changed unexpectedly: %#v", parameter["type_url"])
	}
	if _, exists := parameter["@type"]; exists {
		t.Fatalf("unexpected @type field present: %#v", parameter["@type"])
	}
}

func TestCollectEligibleCandidatesFiltersBSCBelowMinimumBNB(t *testing.T) {
	file := &ChainFile{
		Chain: "bsc",
		Addresses: []AddressRecord{
			{
				Index:       0,
				Address:     "0x1111111111111111111111111111111111111111",
				MnemonicTag: "m-a",
				BNBBalance:  "0.000999",
				USDTBalance: "12.000000",
			},
			{
				Index:       1,
				Address:     "0x2222222222222222222222222222222222222222",
				MnemonicTag: "m-b",
				BNBBalance:  "0.001000",
				USDTBalance: "12.000000",
			},
		},
	}

	candidates, total := collectEligibleCandidates("bsc", file, decimal.RequireFromString("10"), "", "")
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}
	if candidates[0].Address != "0x2222222222222222222222222222222222222222" {
		t.Fatalf("unexpected candidate address: %s", candidates[0].Address)
	}
	if candidates[0].BNBBalance != "0.001000" {
		t.Fatalf("unexpected candidate bnb balance: %s", candidates[0].BNBBalance)
	}
	if !total.Equal(decimal.RequireFromString("12")) {
		t.Fatalf("unexpected total usdt: %s", total.String())
	}
}

func TestCollectEligibleCandidatesFiltersByCurrentMnemonicTag(t *testing.T) {
	file := &ChainFile{
		Chain: "tron",
		Addresses: []AddressRecord{
			{
				Index:       0,
				Address:     "T111111111111111111111111111111111",
				MnemonicTag: "m-old",
				TRXBalance:  "2.000000",
				USDTBalance: "15.000000",
			},
			{
				Index:       1,
				Address:     "T222222222222222222222222222222222",
				MnemonicTag: "m-current",
				TRXBalance:  "2.000000",
				USDTBalance: "18.000000",
			},
		},
	}

	candidates, total := collectEligibleCandidates("tron", file, decimal.RequireFromString("10"), "", "m-current")
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}
	if candidates[0].MnemonicTag != "m-current" {
		t.Fatalf("unexpected mnemonic tag: %s", candidates[0].MnemonicTag)
	}
	if candidates[0].Address != "T222222222222222222222222222222222" {
		t.Fatalf("unexpected candidate address: %s", candidates[0].Address)
	}
	if !total.Equal(decimal.RequireFromString("18")) {
		t.Fatalf("unexpected total usdt: %s", total.String())
	}
}

func TestCollectEligibleCandidatesAllowsTronBalanceEqualToOne(t *testing.T) {
	file := &ChainFile{
		Chain: "tron",
		Addresses: []AddressRecord{
			{
				Index:       0,
				Address:     "T111111111111111111111111111111111",
				MnemonicTag: "m-current",
				TRXBalance:  "1.000000",
				USDTBalance: "15.000000",
			},
			{
				Index:       1,
				Address:     "T222222222222222222222222222222222",
				MnemonicTag: "m-current",
				TRXBalance:  "0.999999",
				USDTBalance: "15.000000",
			},
		},
	}

	candidates, total := collectEligibleCandidates("tron", file, decimal.RequireFromString("10"), "", "m-current")
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}
	if candidates[0].Address != "T111111111111111111111111111111111" {
		t.Fatalf("unexpected candidate address: %s", candidates[0].Address)
	}
	if candidates[0].TRXBalance != "1.000000" {
		t.Fatalf("unexpected candidate trx balance: %s", candidates[0].TRXBalance)
	}
	if !total.Equal(decimal.RequireFromString("15")) {
		t.Fatalf("unexpected total usdt: %s", total.String())
	}
}

func TestCollectMissingWalletIndexesSkipsExistingIndexes(t *testing.T) {
	indexes := collectMissingWalletIndexes([]int{0, 1, 3, 5, 999}, 8, 4)
	expected := []int{2, 4, 6, 7}
	if len(indexes) != len(expected) {
		t.Fatalf("expected %d indexes, got %d", len(expected), len(indexes))
	}
	for i := range expected {
		if indexes[i] != expected[i] {
			t.Fatalf("unexpected index at %d: got %d want %d", i, indexes[i], expected[i])
		}
	}
}
