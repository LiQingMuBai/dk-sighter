package web

import (
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
