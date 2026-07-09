package service

import (
	"context"
	"strings"
	"time"

	"tron_watcher/internal/tron"
)

func RunHourlyBalanceRefresh(ctx context.Context, tronClient *tron.Client, tronBalances *BalanceService, bscScanner *BSCScanner, tronBlockSource string) error {
	loggerTron := tronLogger()
	loggerBSC := bscLogger()
	loc := time.FixedZone("CST", 8*3600)
	perCallDelay := 300 * time.Millisecond
	blockSource := "head"
	if strings.EqualFold(strings.TrimSpace(tronBlockSource), "solid") {
		blockSource = "solid"
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
		blockNumber := int64(0)
		var err error
		if blockSource == "solid" {
			blockNumber, err = tronClient.GetSolidBlockNumber(runCtx)
		} else {
			blockNumber, err = tronClient.GetHeadBlockNumber(runCtx)
		}
		if err != nil {
			loggerTron.Printf("hourly refresh failed: load %s block err=%v", blockSource, err)
		} else {
			loggerTron.Printf("hourly refresh start: source=%s block=%d throttle=%s", blockSource, blockNumber, perCallDelay)
			tronBalances.RefreshAllThrottled(runCtx, blockNumber, perCallDelay)
			loggerTron.Printf("hourly refresh done: source=%s block=%d", blockSource, blockNumber)
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
