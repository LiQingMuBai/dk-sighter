package service

import (
	"context"
	"errors"
	"testing"
)

func TestResolveSyncCursorUsesCurrentWhenLoaderMissing(t *testing.T) {
	got, changed, err := resolveSyncCursor(context.Background(), 123, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if changed {
		t.Fatalf("expected changed=false")
	}
	if got != 123 {
		t.Fatalf("expected current cursor 123, got %d", got)
	}
}

func TestResolveSyncCursorUsesDatabaseValueWhenChanged(t *testing.T) {
	got, changed, err := resolveSyncCursor(context.Background(), 123, func(context.Context) (int64, bool, error) {
		return 99, true, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatalf("expected changed=true")
	}
	if got != 99 {
		t.Fatalf("expected cursor 99, got %d", got)
	}
}

func TestResolveSyncCursorKeepsCurrentWhenDatabaseMissing(t *testing.T) {
	got, changed, err := resolveSyncCursor(context.Background(), 123, func(context.Context) (int64, bool, error) {
		return 0, false, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if changed {
		t.Fatalf("expected changed=false")
	}
	if got != 123 {
		t.Fatalf("expected current cursor 123, got %d", got)
	}
}

func TestResolveSyncCursorReturnsLoaderError(t *testing.T) {
	wantErr := errors.New("boom")
	_, _, err := resolveSyncCursor(context.Background(), 123, func(context.Context) (int64, bool, error) {
		return 0, false, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
}

func TestShouldSkipToLatestBlockWhenLagExceedsThreshold(t *testing.T) {
	if !shouldSkipToLatestBlock(100, 151) {
		t.Fatalf("expected lag 51 to trigger skip")
	}
}

func TestShouldNotSkipToLatestBlockWhenLagWithinThreshold(t *testing.T) {
	if shouldSkipToLatestBlock(100, 150) {
		t.Fatalf("expected lag 50 to keep normal catch-up")
	}
	if shouldSkipToLatestBlock(120, 100) {
		t.Fatalf("expected latest below current to keep current cursor")
	}
}
