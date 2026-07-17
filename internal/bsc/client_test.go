package bsc

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGetUSDTTransfersByRange(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}

		var req struct {
			Method string           `json:"method"`
			Params []map[string]any `json:"params"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Method != "eth_getLogs" {
			t.Fatalf("unexpected method: %s", req.Method)
		}
		if len(req.Params) != 1 {
			t.Fatalf("unexpected params length: %d", len(req.Params))
		}

		filter := req.Params[0]
		if got := filter["fromBlock"]; got != "0x64" {
			t.Fatalf("unexpected fromBlock: %#v", got)
		}
		if got := filter["toBlock"]; got != "0x6e" {
			t.Fatalf("unexpected toBlock: %#v", got)
		}
		if got := filter["address"]; got != "0x55d398326f99059ff775485246999027b3197955" {
			t.Fatalf("unexpected address: %#v", got)
		}

		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":[{"blockNumber":"0x65","transactionHash":"0xAbC","logIndex":"0x2","topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef","0x0000000000000000000000001111111111111111111111111111111111111111","0x0000000000000000000000002222222222222222222222222222222222222222"],"data":"0xde0b6b3a7640000"}]}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, "", "0x55d398326f99059ff775485246999027b3197955")
	client.SetMinRequestInterval(time.Nanosecond)

	transfers, err := client.GetUSDTTransfersByRange(context.Background(), 100, 110)
	if err != nil {
		t.Fatalf("GetUSDTTransfersByRange error: %v", err)
	}
	if len(transfers) != 1 {
		t.Fatalf("unexpected transfer count: %d", len(transfers))
	}
	transfer := transfers[0]
	if transfer.BlockNumber != 101 {
		t.Fatalf("unexpected block number: %d", transfer.BlockNumber)
	}
	if transfer.TxHash != "0xabc" {
		t.Fatalf("unexpected tx hash: %s", transfer.TxHash)
	}
	if transfer.From != "0x1111111111111111111111111111111111111111" {
		t.Fatalf("unexpected from: %s", transfer.From)
	}
	if transfer.To != "0x2222222222222222222222222222222222222222" {
		t.Fatalf("unexpected to: %s", transfer.To)
	}
	if transfer.LogIndex != 2 {
		t.Fatalf("unexpected log index: %d", transfer.LogIndex)
	}
	if transfer.Value == nil || transfer.Value.String() != "1000000000000000000" {
		t.Fatalf("unexpected value: %v", transfer.Value)
	}
}
