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
	tronInterval := 40 * time.Minute
	tronOffset := 10 * time.Minute
	blockSource := "head"
	if strings.EqualFold(strings.TrimSpace(tronBlockSource), "solid") {
		blockSource = "solid"
	}

	for {
		now := time.Now().In(loc)
		nextTron := nextOffsetMinuteBoundary(now, 40, 10)
		nextBSC := nextHourlyBoundary(now)
		next := nextTron
		if nextBSC.Before(next) {
			next = nextBSC
		}
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		runAt := time.Now().In(loc)
		if !runAt.Before(nextTron) {
			runCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
			blockNumber := int64(0)
			var err error
			if blockSource == "solid" {
				blockNumber, err = tronClient.GetSolidBlockNumber(runCtx)
			} else {
				blockNumber, err = tronClient.GetHeadBlockNumber(runCtx)
			}
			if err != nil {
				loggerTron.Printf("scheduled refresh failed: interval=%s offset=%s load %s block err=%v", tronInterval, tronOffset, blockSource, err)
			} else {
				loggerTron.Printf("scheduled refresh start: interval=%s offset=%s source=%s block=%d throttle=%s", tronInterval, tronOffset, blockSource, blockNumber, perCallDelay)
				tronBalances.RefreshAllThrottled(runCtx, blockNumber, perCallDelay)
				loggerTron.Printf("scheduled refresh done: interval=%s offset=%s source=%s block=%d", tronInterval, tronOffset, blockSource, blockNumber)
			}
			cancel()
		}

		if !runAt.Before(nextBSC) {
			runCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
			if bscScanner != nil {
				loggerBSC.Printf("scheduled refresh start: interval=1h throttle=%s", perCallDelay)
				bscScanner.RefreshAllBalancesThrottled(runCtx, perCallDelay)
				loggerBSC.Printf("scheduled refresh done: interval=1h")
			} else {
				loggerBSC.Printf("scheduled refresh skipped: interval=1h bsc scanner disabled")
			}
			cancel()
		}
	}
}

func nextHourlyBoundary(now time.Time) time.Time {
	return time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+1, 0, 0, 0, now.Location())
}

func nextOffsetMinuteBoundary(now time.Time, stepMinutes int, offsetMinutes int) time.Time {
	if stepMinutes <= 0 {
		stepMinutes = 1
	}
	if offsetMinutes < 0 {
		offsetMinutes = 0
	}
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	first := startOfDay.Add(time.Duration(offsetMinutes) * time.Minute)
	if now.Before(first) {
		return first
	}
	elapsed := now.Sub(first)
	step := time.Duration(stepMinutes) * time.Minute
	nextSteps := elapsed/step + 1
	return first.Add(nextSteps * step)
}
