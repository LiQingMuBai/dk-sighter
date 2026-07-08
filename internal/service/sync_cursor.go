package service

import "context"

const maxAllowedSyncLagBlocks int64 = 20

type loadLastBlockFunc func(context.Context) (int64, bool, error)

func resolveSyncCursor(ctx context.Context, current int64, load loadLastBlockFunc) (int64, bool, error) {
	if load == nil {
		return current, false, nil
	}
	dbLast, exists, err := load(ctx)
	if err != nil {
		return 0, false, err
	}
	if !exists {
		return current, false, nil
	}
	if dbLast == current {
		return current, false, nil
	}
	return dbLast, true, nil
}

func shouldSkipToLatestBlock(current, latest int64) bool {
	if latest <= current {
		return false
	}
	return latest-current > maxAllowedSyncLagBlocks
}
