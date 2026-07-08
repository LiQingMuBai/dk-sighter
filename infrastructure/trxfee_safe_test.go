package infrastructure

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTrxfeeOrderSafeUsesNewOrderAPI(t *testing.T) {
	type requestBody struct {
		EnergyAmount   int    `json:"energy_amount"`
		Period         string `json:"period"`
		ReceiveAddress string `json:"receive_address"`
		CallbackURL    string `json:"callback_url"`
		OutTradeNo     string `json:"out_trade_no"`
		AutoActivation bool   `json:"auto_activation"`
	}

	var (
		gotPath      string
		gotMethod    string
		gotHeader    string
		gotAPIHeader string
		gotBody      requestBody
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotMethod = r.Method
		gotHeader = r.Header.Get("Content-Type")
		gotAPIHeader = r.Header.Get("X-API-Key")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer server.Close()

	client := NewTrxfeeClient(server.URL, "", "")
	resp, err := client.OrderSafe("ORDER-001", "TTESTADDR", 65000)
	if err != nil {
		t.Fatalf("OrderSafe returned error: %v", err)
	}
	if resp != `{"success":true}` {
		t.Fatalf("unexpected response: %s", resp)
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("unexpected method: %s", gotMethod)
	}
	if gotPath != "/api/trxfee/order" {
		t.Fatalf("unexpected path: %s", gotPath)
	}
	if gotHeader != "application/json" {
		t.Fatalf("unexpected content-type: %s", gotHeader)
	}
	if gotAPIHeader != "masion" {
		t.Fatalf("unexpected X-API-Key: %s", gotAPIHeader)
	}
	if gotBody.EnergyAmount != 65000 {
		t.Fatalf("unexpected energy amount: %d", gotBody.EnergyAmount)
	}
	if gotBody.Period != "1H" {
		t.Fatalf("unexpected period: %s", gotBody.Period)
	}
	if gotBody.ReceiveAddress != "TTESTADDR" {
		t.Fatalf("unexpected receive address: %s", gotBody.ReceiveAddress)
	}
	if gotBody.OutTradeNo != "ORDER-001" {
		t.Fatalf("unexpected outTradeNo: %s", gotBody.OutTradeNo)
	}
	if !gotBody.AutoActivation {
		t.Fatalf("expected auto_activation to be true")
	}
}

func TestTrxfeeOrderEndpointKeepsFullPath(t *testing.T) {
	client := NewTrxfeeClient("https://usdtee.xyz/api/trxfee/order", "", "")
	if got := client.orderEndpoint(); got != "https://usdtee.xyz/api/trxfee/order" {
		t.Fatalf("unexpected endpoint: %s", got)
	}
}
