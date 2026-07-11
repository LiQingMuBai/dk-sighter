package service

import (
	"context"
	"strings"
	"time"
)

func RunHourlyBalanceRefresh(ctx context.Context, tronBalances *BalanceService, bscScanner *BSCScanner, tronBlockSource string, tracker *ScheduledRefreshStateTracker, manualTracker *ScheduledRefreshStateTracker) error {
	loggerTron := tronLogger()
	loggerBSC := bscLogger()
	loc := time.FixedZone("CST", 8*3600)
	perCallDelay := 300 * time.Millisecond
	tronInterval := 40 * time.Minute
	tronOffset := 10 * time.Minute
	bscInterval := 30 * time.Minute
	blockSource := "head"
	if strings.EqualFold(strings.TrimSpace(tronBlockSource), "solid") {
		blockSource = "solid"
	}

	for {
		now := time.Now().In(loc)
		nextTron := nextOffsetMinuteBoundary(now, 40, 10)
		nextBSC := nextOffsetMinuteBoundary(now, 30, 0)
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
		scheduledRefreshStarted := false
		if !runAt.Before(nextTron) {
			if scheduledRefreshStarted {
				loggerTron.Printf("scheduled refresh skipped: interval=%s offset=%s reason=another scheduled refresh already started", tronInterval, tronOffset)
			} else if activeManualChains := manualTracker.ActiveChains(); len(activeManualChains) > 0 {
				loggerTron.Printf("scheduled refresh skipped: interval=%s offset=%s reason=manual full refresh is running active=%s", tronInterval, tronOffset, strings.Join(activeManualChains, ","))
			} else {
				started := false
				func() {
					if tracker != nil {
						ok, activeChains := tracker.TryStart("tron")
						if !ok {
							loggerTron.Printf("scheduled refresh skipped: interval=%s offset=%s reason=another scheduled refresh is running active=%s", tronInterval, tronOffset, strings.Join(activeChains, ","))
							return
						}
						started = true
						defer tracker.Finish("tron")
					}
					runCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
					defer cancel()
					blockNumber := int64(0)
					var err error
					if tronBalances == nil || tronBalances.tronClient == nil {
						loggerTron.Printf("scheduled refresh skipped: interval=%s offset=%s reason=tron balance refresher disabled", tronInterval, tronOffset)
					} else {
						started = true
						if blockSource == "solid" {
							blockNumber, err = tronBalances.tronClient.GetSolidBlockNumber(runCtx)
						} else {
							blockNumber, err = tronBalances.tronClient.GetHeadBlockNumber(runCtx)
						}
						if err != nil {
							loggerTron.Printf("scheduled refresh failed: interval=%s offset=%s load %s block err=%v", tronInterval, tronOffset, blockSource, err)
						} else {
							loggerTron.Printf("scheduled refresh start: interval=%s offset=%s source=%s block=%d throttle=%s", tronInterval, tronOffset, blockSource, blockNumber, perCallDelay)
							tronBalances.RefreshAllThrottled(runCtx, blockNumber, perCallDelay)
							loggerTron.Printf("scheduled refresh done: interval=%s offset=%s source=%s block=%d", tronInterval, tronOffset, blockSource, blockNumber)
						}
					}
				}()
				if started {
					scheduledRefreshStarted = true
				}
			}
		}

		if !runAt.Before(nextBSC) {
			if scheduledRefreshStarted {
				loggerBSC.Printf("scheduled refresh skipped: interval=%s reason=another scheduled refresh already started", bscInterval)
			} else if activeManualChains := manualTracker.ActiveChains(); len(activeManualChains) > 0 {
				loggerBSC.Printf("scheduled refresh skipped: interval=%s reason=manual full refresh is running active=%s", bscInterval, strings.Join(activeManualChains, ","))
			} else {
				started := false
				func() {
					if tracker != nil {
						ok, activeChains := tracker.TryStart("bsc")
						if !ok {
							loggerBSC.Printf("scheduled refresh skipped: interval=%s reason=another scheduled refresh is running active=%s", bscInterval, strings.Join(activeChains, ","))
							return
						}
						started = true
						defer tracker.Finish("bsc")
					}
					runCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
					defer cancel()
					if bscScanner != nil {
						started = true
						loggerBSC.Printf("scheduled refresh start: interval=%s throttle=%s", bscInterval, perCallDelay)
						bscScanner.RefreshAllBalancesThrottled(runCtx, perCallDelay)
						loggerBSC.Printf("scheduled refresh done: interval=%s", bscInterval)
					} else {
						loggerBSC.Printf("scheduled refresh skipped: interval=%s bsc scanner disabled", bscInterval)
					}
				}()
				if started {
					scheduledRefreshStarted = true
				}
			}
		}
	}
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
