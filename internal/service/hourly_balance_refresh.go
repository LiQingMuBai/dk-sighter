package service

import (
	"context"
	"time"

	"tron_watcher/internal/tron"
)

func RunHourlyBalanceRefresh(ctx context.Context, tronClient *tron.Client, tronBalances *BalanceService, bscScanner *BSCScanner) error {
	loggerTron := tronLogger()
	loggerBSC := bscLogger()
	loc := time.FixedZone("CST", 8*3600)
	perCallDelay := 300 * time.Millisecond

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
		solid, err := tronClient.GetSolidBlockNumber(runCtx)
		if err != nil {
			loggerTron.Printf("hourly refresh failed: load solid block err=%v", err)
		} else {
			loggerTron.Printf("hourly refresh start: solid=%d throttle=%s", solid, perCallDelay)
			tronBalances.RefreshAllThrottled(runCtx, solid, perCallDelay)
			loggerTron.Printf("hourly refresh done: solid=%d", solid)
		}

		if bscScanner != nil {
			loggerBSC.Printf("hourly refresh start: throttle=%s", perCallDelay)
			bscScanner.RefreshAllBalancesThrottled(runCtx, perCallDelay)
			loggerBSC.Printf("hourly refresh done")
		} else {
			loggerBSC.Printf("hourly refresh skipped: bsc scanner disabled")
		}
		cancel()
	}
}
