package tron

import (
	"context"
	"errors"
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
