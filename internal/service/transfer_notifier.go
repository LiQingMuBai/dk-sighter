package service

import (
	"context"

	"golang.org/x/sync/errgroup"

	"tron_watcher/internal/repository"
)

type TransferNotifier interface {
	NotifyTransfer(ctx context.Context, chain string, direction string, record repository.TransferRecord)
	Run(ctx context.Context) error
}

type MultiTransferNotifier struct {
	notifiers []TransferNotifier
}

func NewMultiTransferNotifier(notifiers ...TransferNotifier) TransferNotifier {
	filtered := make([]TransferNotifier, 0, len(notifiers))
	for _, notifier := range notifiers {
		if notifier != nil {
			filtered = append(filtered, notifier)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	if len(filtered) == 1 {
		return filtered[0]
	}
	return &MultiTransferNotifier{notifiers: filtered}
}

func (n *MultiTransferNotifier) NotifyTransfer(ctx context.Context, chain string, direction string, record repository.TransferRecord) {
	for _, notifier := range n.notifiers {
		notifier.NotifyTransfer(ctx, chain, direction, record)
	}
}

func (n *MultiTransferNotifier) Run(ctx context.Context) error {
	group, groupCtx := errgroup.WithContext(ctx)
	for _, notifier := range n.notifiers {
		current := notifier
		group.Go(func() error {
			return current.Run(groupCtx)
		})
	}
	return group.Wait()
}
