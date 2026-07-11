package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type stubScheduledRefreshStatus struct {
	chains []string
}

func (s stubScheduledRefreshStatus) ActiveChains() []string {
	result := make([]string, 0, len(s.chains))
	result = append(result, s.chains...)
	return result
}

type stubManualRefreshStatus struct {
	chains []string
}

func (s stubManualRefreshStatus) ActiveChains() []string {
	result := make([]string, 0, len(s.chains))
	result = append(result, s.chains...)
	return result
}

func (s stubManualRefreshStatus) Start(string)  {}
func (s stubManualRefreshStatus) Finish(string) {}

type stubAddressReloader struct {
	addresses []string
}

func (s stubAddressReloader) Reload(context.Context) error { return nil }
func (s stubAddressReloader) List() []string {
	result := make([]string, 0, len(s.addresses))
	result = append(result, s.addresses...)
	return result
}

type stubTronBalanceRefresher struct{}

func (s stubTronBalanceRefresher) RefreshAddresses(context.Context, []string) error {
	return nil
}

func (s stubTronBalanceRefresher) RefreshAddressesWithPositiveTRX(context.Context, []string) error {
	return nil
}

type stubBSCBalanceRefresher struct{}

func (s stubBSCBalanceRefresher) RefreshAddresses(context.Context, []string) {}

func TestStartManualRefreshAllBlockedWhenScheduledRefreshRunning(t *testing.T) {
	server := &Server{
		scheduledRefreshStatus: stubScheduledRefreshStatus{
			chains: []string{"tron", "bsc"},
		},
	}

	_, err := server.startManualRefreshAll("tron")
	if err == nil {
		t.Fatalf("expected error when scheduled refresh is running")
	}
	if !strings.Contains(err.Error(), "定时刷新任务正在执行中") {
		t.Fatalf("expected scheduled refresh warning, got %v", err)
	}
	if !strings.Contains(err.Error(), "TRON, BSC") && !strings.Contains(err.Error(), "BSC, TRON") {
		t.Fatalf("expected active chains in error, got %v", err)
	}
}

func TestStartManualRefreshAllBlockedWhenAnotherManualRefreshRunning(t *testing.T) {
	server := &Server{
		tronManualRefresh: manualBalanceRefreshJob{
			running: true,
		},
	}

	_, err := server.startManualRefreshAll("bsc")
	if err == nil {
		t.Fatalf("expected error when another manual refresh is running")
	}
	if !strings.Contains(err.Error(), "手动全量更新任务正在执行中") {
		t.Fatalf("expected manual refresh warning, got %v", err)
	}
	if !strings.Contains(err.Error(), "TRON") {
		t.Fatalf("expected active manual chain in error, got %v", err)
	}
}

func TestHandleManualRefreshStatusIncludesScheduledRunning(t *testing.T) {
	server := &Server{
		scheduledRefreshStatus: stubScheduledRefreshStatus{
			chains: []string{"tron"},
		},
		manualRefreshStatus: stubManualRefreshStatus{},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/manual-refresh-status", nil)
	rec := httptest.NewRecorder()
	server.handleManualRefreshStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp manualRefreshStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success response")
	}
	if !resp.ScheduledRunning {
		t.Fatalf("expected scheduled_running=true")
	}
	if resp.ManualRunning {
		t.Fatalf("expected manual_running=false")
	}
	if len(resp.ScheduledActiveChains) != 1 || resp.ScheduledActiveChains[0] != "TRON" {
		t.Fatalf("expected scheduled active chains [TRON], got %#v", resp.ScheduledActiveChains)
	}
}

func TestStartManualRefreshAllRequiresTronManualRefresher(t *testing.T) {
	server := &Server{
		reloader: stubAddressReloader{
			addresses: []string{"TTestAddress"},
		},
	}

	_, err := server.startManualRefreshAll("tron")
	if err == nil {
		t.Fatalf("expected error when tron manual refresher is not configured")
	}
	if !strings.Contains(err.Error(), "tron manual balance refresher not configured") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStartManualRefreshAllUsesTronManualRefresherForStartupCheck(t *testing.T) {
	server := &Server{
		reloader: stubAddressReloader{
			addresses: []string{"TTestAddress"},
		},
		tronBalances:       nil,
		tronManualBalances: stubTronBalanceRefresher{},
	}

	total, err := server.startManualRefreshAll("tron")
	if err != nil {
		t.Fatalf("expected manual refresh to start, got error: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected total=1, got %d", total)
	}
}

func TestStartManualRefreshAllRequiresBSCManualRefresher(t *testing.T) {
	server := &Server{}

	_, err := server.startManualRefreshAll("bsc")
	if err == nil {
		t.Fatalf("expected error when bsc manual refresher is not configured")
	}
	if !strings.Contains(err.Error(), "bsc manual balance refresher not configured") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStartManualRefreshAllUsesBSCManualRefresherForStartupCheck(t *testing.T) {
	server := &Server{
		bscBalances:       nil,
		bscManualBalances: stubBSCBalanceRefresher{},
		repo:              nil,
	}

	_, err := server.startManualRefreshAll("bsc")
	if err == nil {
		t.Fatalf("expected repository error after passing bsc manual refresher check")
	}
	if strings.Contains(err.Error(), "bsc manual balance refresher not configured") {
		t.Fatalf("expected startup check to use bscManualBalances, got error: %v", err)
	}
	if !strings.Contains(err.Error(), "bsc repository not configured") {
		t.Fatalf("expected repository error, got %v", err)
	}
}
