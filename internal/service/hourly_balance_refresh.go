package service

import (
	"context"
	"time"

	"tron_watcher/internal/tron"
)

func RunHourlyBalanceRefresh(ctx context.Context, tronClient *tron.Client, tronBalances *BalanceService, tronDelay time.Duration, bscScanner *BSCScanner, bscDelay time.Duration) error {
	loggerTron := tronLogger()
	loggerBSC := bscLogger()
	loc := time.FixedZone("CST", 8*3600)
	if tronDelay <= 0 {
		tronDelay = 10 * time.Millisecond
	}
	if bscDelay <= 0 {
		bscDelay = 10 * time.Millisecond
	}

	for {
		now := time.Now().In(loc)
		next := time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+1, 0, 0, 0, loc)
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		runCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
		if tronClient != nil && tronBalances != nil {
			solid, err := tronClient.GetSolidBlockNumber(runCtx)
			if err != nil {
				loggerTron.Printf("hourly refresh failed: load solid block err=%v", err)
			} else {
				loggerTron.Printf("hourly refresh start: solid=%d throttle=%s", solid, tronDelay)
				tronBalances.RefreshAllActivatedThrottled(runCtx, solid, tronDelay)
				loggerTron.Printf("hourly refresh done: solid=%d", solid)
			}
		} else {
			loggerTron.Printf("hourly refresh skipped: tron scheduled sync disabled")
		}

		if bscScanner != nil {
			loggerBSC.Printf("hourly refresh start: throttle=%s", bscDelay)
			bscScanner.RefreshAllBalancesThrottled(runCtx, bscDelay)
			loggerBSC.Printf("hourly refresh done")
		} else {
			loggerBSC.Printf("hourly refresh skipped: bsc scanner disabled")
		}
		cancel()
	}
}
