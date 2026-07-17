package tron

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDetectRPCError(t *testing.T) {
	err := detectRPCError([]byte(`{"code":-32007,"message":"50/second request limit reached"}`))
	if err == nil {
		t.Fatal("expected rpc error")
	}
	if !strings.Contains(err.Error(), "-32007") {
		t.Fatalf("expected error code in message, got %v", err)
	}
}

func TestDetectRPCErrorIgnoresSuccessPayload(t *testing.T) {
	err := detectRPCError([]byte(`{"block_header":{"raw_data":{"number":123}}}`))
	if err != nil {
		t.Fatalf("expected nil error for success payload, got %v", err)
	}
}

func TestClientPostReturnsRPCError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":-32007,"message":"50/second request limit reached"}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, "", "", 10*time.Millisecond)

	var out map[string]any
	err := client.post(context.Background(), "/walletsolidity/getnowblock", map[string]any{}, &out)
	if err == nil {
		t.Fatal("expected rpc error")
	}
	if !strings.Contains(err.Error(), "50/second request limit reached") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClientWaitTurnHonorsContextCancel(t *testing.T) {
	client := NewClient("https://example.com", "", "", 10*time.Millisecond)
	client.nextRequest = time.Now().Add(100 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.waitTurn(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestEnsureRawDataHexBackfillsMissingField(t *testing.T) {
	input := []byte(`{
		"visible": false,
		"txID": "ae80c7aa55e19c2da5e712d4be50e4c9422dd6bfc99039cc42d5deb76938c0e7",
		"raw_data": {
			"contract": [{
				"parameter": {
					"value": {
						"amount": 1000,
						"owner_address": "41eca9bc828a3005b9a3b909f2cc5c2a54794de05f",
						"to_address": "41495511a493d8c362be4267224e6d81013a6862ee"
					},
					"type_url": "type.googleapis.com/protocol.TransferContract"
				},
				"type": "TransferContract"
			}],
			"ref_block_bytes": "8acb",
			"ref_block_hash": "7c571ea3e7e9dbf3",
			"expiration": 1773182925000,
			"timestamp": 1773182865805
		}
	}`)

	got, err := ensureRawDataHex(input)
	if err != nil {
		t.Fatalf("ensureRawDataHex returned error: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(got, &payload); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}

	rawDataHex, ok := payload["raw_data_hex"].(string)
	if !ok || strings.TrimSpace(rawDataHex) == "" {
		t.Fatalf("expected raw_data_hex to be backfilled, got %#v", payload["raw_data_hex"])
	}
	if payload["txID"] != "ae80c7aa55e19c2da5e712d4be50e4c9422dd6bfc99039cc42d5deb76938c0e7" {
		t.Fatalf("unexpected txID: %#v", payload["txID"])
	}
}

func TestShouldInspectUSDTTriggerTx(t *testing.T) {
	client := NewClient("https://example.com", "", "412222222222222222222222222222222222222222", 10*time.Millisecond)
	watched := map[string]struct{}{
		NormalizeHexAddress("41AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"): {},
		NormalizeHexAddress("41BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"): {},
	}
	isWatchedHex := func(hexAddr string) bool {
		_, ok := watched[NormalizeHexAddress(hexAddr)]
		return ok
	}

	tests := []struct {
		name string
		tx   Transaction
		want bool
	}{
		{
			name: "direct transfer to watched address",
			tx: newTriggerSmartContractTx(
				"412222222222222222222222222222222222222222",
				"41CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
				trc20TransferMethodID+encodeAddressArg("41AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")+strings.Repeat("0", 64),
			),
			want: true,
		},
		{
			name: "direct transfer from watched owner",
			tx: newTriggerSmartContractTx(
				"412222222222222222222222222222222222222222",
				"41BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
				trc20TransferMethodID+encodeAddressArg("41DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD")+strings.Repeat("0", 64),
			),
			want: true,
		},
		{
			name: "direct transfer without watched addresses",
			tx: newTriggerSmartContractTx(
				"412222222222222222222222222222222222222222",
				"41CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
				trc20TransferMethodID+encodeAddressArg("41DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD")+strings.Repeat("0", 64),
			),
			want: false,
		},
		{
			name: "transferFrom with watched source",
			tx: newTriggerSmartContractTx(
				"412222222222222222222222222222222222222222",
				"41EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE",
				trc20TransferFromMethodID+
					encodeAddressArg("41AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")+
					encodeAddressArg("41DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD")+
					strings.Repeat("0", 64),
			),
			want: true,
		},
		{
			name: "non usdt contract is skipped",
			tx: newTriggerSmartContractTx(
				"413333333333333333333333333333333333333333",
				"41AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
				trc20TransferMethodID+encodeAddressArg("41BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")+strings.Repeat("0", 64),
			),
			want: false,
		},
		{
			name: "unknown method on usdt contract is skipped",
			tx: newTriggerSmartContractTx(
				"412222222222222222222222222222222222222222",
				"41AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
				"095ea7b3"+encodeAddressArg("41BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")+strings.Repeat("0", 64),
			),
			want: false,
		},
		{
			name: "malformed calldata stays conservative",
			tx: newTriggerSmartContractTx(
				"412222222222222222222222222222222222222222",
				"41AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
				"abcd",
			),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := client.ShouldInspectUSDTTriggerTx(tt.tx, isWatchedHex)
			if got != tt.want {
				t.Fatalf("ShouldInspectUSDTTriggerTx() = %v, want %v", got, tt.want)
			}
		})
	}
}

func newTriggerSmartContractTx(contractAddress, ownerAddress, data string) Transaction {
	payload := fmt.Sprintf(`{
		"txID":"txid",
		"raw_data":{
			"contract":[{
				"type":"TriggerSmartContract",
				"parameter":{
					"value":{
						"owner_address":"%s",
						"contract_address":"%s",
						"data":"%s"
					}
				}
			}]
		}
	}`, ownerAddress, contractAddress, data)

	var tx Transaction
	if err := json.Unmarshal([]byte(payload), &tx); err != nil {
		panic(err)
	}
	return tx
}

func encodeAddressArg(hexAddr string) string {
	clean := strings.TrimPrefix(strings.ToLower(NormalizeHexAddress(hexAddr)), "41")
	return strings.Repeat("0", 24) + clean
}
