package service

import "testing"

func TestScheduledRefreshStateTrackerActiveChains(t *testing.T) {
	tracker := NewScheduledRefreshStateTracker()

	tracker.Start(" tron ")
	tracker.Start("bsc")

	got := tracker.ActiveChains()
	if len(got) != 2 {
		t.Fatalf("expected 2 active chains, got %d: %#v", len(got), got)
	}
	if got[0] != "bsc" || got[1] != "tron" {
		t.Fatalf("expected sorted active chains [bsc tron], got %#v", got)
	}

	tracker.Finish("tron")
	got = tracker.ActiveChains()
	if len(got) != 1 || got[0] != "bsc" {
		t.Fatalf("expected only bsc to remain active, got %#v", got)
	}
}

func TestScheduledRefreshStateTrackerTryStartBlocksOtherChains(t *testing.T) {
	tracker := NewScheduledRefreshStateTracker()

	ok, active := tracker.TryStart("tron")
	if !ok {
		t.Fatalf("expected first scheduled refresh to start, active=%#v", active)
	}

	ok, active = tracker.TryStart("bsc")
	if ok {
		t.Fatalf("expected second scheduled refresh to be blocked")
	}
	if len(active) != 1 || active[0] != "tron" {
		t.Fatalf("expected tron to remain active, got %#v", active)
	}

	tracker.Finish("tron")

	ok, active = tracker.TryStart("bsc")
	if !ok {
		t.Fatalf("expected bsc scheduled refresh to start after tron finished, active=%#v", active)
	}
}
