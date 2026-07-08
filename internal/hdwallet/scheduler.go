package hdwallet

import (
	"context"
	"log"
	"time"
)

var beijingLocation = time.FixedZone("CST", 8*3600)

func (s *Service) RunTronHourlyRefresh(ctx context.Context) error {
	for {
		next := nextHourlyBoundary(time.Now().In(beijingLocation))
		log.Printf("hd wallet tron hourly refresh scheduled at %s", next.Format("2006-01-02 15:04:05"))
		if err := waitUntil(ctx, next); err != nil {
			return err
		}
		if err := s.runScheduledSync(ctx, "tron"); err != nil && err != context.Canceled {
			return err
		}
	}
}

func (s *Service) RunBSCFiveHourRefresh(ctx context.Context) error {
	for {
		next := nextNHourBoundary(time.Now().In(beijingLocation), 5)
		log.Printf("hd wallet bsc five-hour refresh scheduled at %s", next.Format("2006-01-02 15:04:05"))
		if err := waitUntil(ctx, next); err != nil {
			return err
		}
		if err := s.runScheduledSync(ctx, "bsc"); err != nil && err != context.Canceled {
			return err
		}
	}
}

func nextHourlyBoundary(now time.Time) time.Time {
	return time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+1, 0, 0, 0, now.Location())
}

func nextNHourBoundary(now time.Time, step int) time.Time {
	current := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, now.Location())
	nextHour := ((current.Hour() / step) + 1) * step
	return time.Date(current.Year(), current.Month(), current.Day(), nextHour, 0, 0, 0, current.Location())
}

func waitUntil(ctx context.Context, next time.Time) error {
	timer := time.NewTimer(time.Until(next))
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
