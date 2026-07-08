package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"
)

const HDWalletSource = "hd_wallet"

type HDWatchAddressInput struct {
	WalletIndex int
	MnemonicTag string
	Address     string
}

type HDDashboardRow struct {
	WalletIndex int
	MnemonicTag string
	Address     string
	TRXBalance  decimal.Decimal
	USDTBalance decimal.Decimal
	BNBBalance  decimal.Decimal
	UpdatedAt   sql.NullTime
}

type HDSummary struct {
	Count       int
	TRXTotal    decimal.Decimal
	USDTTotal   decimal.Decimal
	BNBTotal    decimal.Decimal
	LastUpdated sql.NullTime
}

func (d *DB) InsertHDTronWatchAddresses(ctx context.Context, source string, items []HDWatchAddressInput) error {
	return d.insertHDTronWatchAddresses(ctx, source, items)
}

func (d *DB) InsertHDBSCWatchAddresses(ctx context.Context, source string, items []HDWatchAddressInput) error {
	return d.insertHDBSCWatchAddresses(ctx, source, items)
}

func (d *DB) insertHDTronWatchAddresses(ctx context.Context, source string, items []HDWatchAddressInput) error {
	source = normalizeHDSource(source)
	if len(items) == 0 {
		return nil
	}

	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx for hd tron watch addresses: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO watch_addresses (address_base58, source, wallet_index, mnemonic_tag, status)
		VALUES (?, ?, ?, ?, 1)
		ON DUPLICATE KEY UPDATE
			source = VALUES(source),
			wallet_index = VALUES(wallet_index),
			mnemonic_tag = VALUES(mnemonic_tag),
			status = 1,
			updated_at = CURRENT_TIMESTAMP
	`)
	if err != nil {
		return fmt.Errorf("prepare insert hd tron watch addresses: %w", err)
	}
	defer stmt.Close()

	for _, item := range items {
		address := strings.TrimSpace(item.Address)
		if address == "" {
			continue
		}
		if _, err := stmt.ExecContext(ctx, address, source, item.WalletIndex, strings.TrimSpace(item.MnemonicTag)); err != nil {
			return fmt.Errorf("insert hd tron watch address %s: %w", address, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit hd tron watch addresses: %w", err)
	}
	return nil
}

func (d *DB) insertHDBSCWatchAddresses(ctx context.Context, source string, items []HDWatchAddressInput) error {
	source = normalizeHDSource(source)
	if len(items) == 0 {
		return nil
	}

	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx for hd bsc watch addresses: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO bsc_watch_addresses (address, source, wallet_index, mnemonic_tag, status)
		VALUES (?, ?, ?, ?, 1)
		ON DUPLICATE KEY UPDATE
			source = VALUES(source),
			wallet_index = VALUES(wallet_index),
			mnemonic_tag = VALUES(mnemonic_tag),
			status = 1,
			updated_at = CURRENT_TIMESTAMP
	`)
	if err != nil {
		return fmt.Errorf("prepare insert hd bsc watch addresses: %w", err)
	}
	defer stmt.Close()

	for _, item := range items {
		address := strings.ToLower(strings.TrimSpace(item.Address))
		if address == "" {
			continue
		}
		if _, err := stmt.ExecContext(ctx, address, source, item.WalletIndex, strings.TrimSpace(item.MnemonicTag)); err != nil {
			return fmt.Errorf("insert hd bsc watch address %s: %w", address, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit hd bsc watch addresses: %w", err)
	}
	return nil
}

func (d *DB) ListHDTronDashboardRows(ctx context.Context, source string, limit, offset int) ([]HDDashboardRow, int, error) {
	source = normalizeHDSource(source)
	total, err := d.countWatchAddressesBySource(ctx, "watch_addresses", source)
	if err != nil {
		return nil, 0, err
	}

	rows, err := d.sql.QueryContext(ctx, `
		SELECT
			w.wallet_index,
			w.mnemonic_tag,
			w.address_base58,
			COALESCE(trx.balance, 0) AS trx_balance,
			COALESCE(usdt.balance, 0) AS usdt_balance,
			CASE
				WHEN trx.updated_at IS NULL AND usdt.updated_at IS NULL THEN w.updated_at
				WHEN trx.updated_at IS NULL THEN usdt.updated_at
				WHEN usdt.updated_at IS NULL THEN trx.updated_at
				WHEN trx.updated_at >= usdt.updated_at THEN trx.updated_at
				ELSE usdt.updated_at
			END AS updated_at
		FROM watch_addresses w
		LEFT JOIN asset_balances trx
			ON trx.address_base58 = w.address_base58
			AND trx.asset_code = 'TRX'
		LEFT JOIN asset_balances usdt
			ON usdt.address_base58 = w.address_base58
			AND usdt.asset_code = 'USDT'
		WHERE w.status = 1
		  AND w.source = ?
		ORDER BY w.wallet_index ASC, w.id ASC
		LIMIT ? OFFSET ?
	`, source, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list hd tron dashboard rows: %w", err)
	}
	defer rows.Close()

	result := make([]HDDashboardRow, 0, limit)
	for rows.Next() {
		var item HDDashboardRow
		var trxBalance string
		var usdtBalance string
		var updatedAt sql.NullTime
		if err := rows.Scan(&item.WalletIndex, &item.MnemonicTag, &item.Address, &trxBalance, &usdtBalance, &updatedAt); err != nil {
			return nil, 0, fmt.Errorf("scan hd tron dashboard row: %w", err)
		}
		item.UpdatedAt = normalizeNullTime(updatedAt)
		item.TRXBalance, err = decimal.NewFromString(trxBalance)
		if err != nil {
			return nil, 0, fmt.Errorf("parse hd tron trx balance: %w", err)
		}
		item.USDTBalance, err = decimal.NewFromString(usdtBalance)
		if err != nil {
			return nil, 0, fmt.Errorf("parse hd tron usdt balance: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return result, total, nil
}

func (d *DB) ListHDBSCDashboardRows(ctx context.Context, source string, limit, offset int) ([]HDDashboardRow, int, error) {
	source = normalizeHDSource(source)
	total, err := d.countWatchAddressesBySource(ctx, "bsc_watch_addresses", source)
	if err != nil {
		return nil, 0, err
	}

	rows, err := d.sql.QueryContext(ctx, `
		SELECT
			w.wallet_index,
			w.mnemonic_tag,
			w.address,
			COALESCE(bnb.balance, 0) AS bnb_balance,
			COALESCE(usdt.balance, 0) AS usdt_balance,
			CASE
				WHEN bal.updated_at IS NOT NULL THEN bal.updated_at
				ELSE w.updated_at
			END AS updated_at
		FROM bsc_watch_addresses w
		LEFT JOIN (
			SELECT LOWER(address) AS address, MAX(updated_at) AS updated_at
			FROM bsc_asset_balances
			GROUP BY LOWER(address)
		) bal
			ON bal.address = LOWER(w.address)
		LEFT JOIN bsc_asset_balances bnb
			ON LOWER(bnb.address) = LOWER(w.address) AND bnb.asset_code = 'BNB'
		LEFT JOIN bsc_asset_balances usdt
			ON LOWER(usdt.address) = LOWER(w.address) AND usdt.asset_code = 'USDT'
		WHERE w.status = 1
		  AND w.source = ?
		ORDER BY w.wallet_index ASC, w.id ASC
		LIMIT ? OFFSET ?
	`, source, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list hd bsc dashboard rows: %w", err)
	}
	defer rows.Close()

	result := make([]HDDashboardRow, 0, limit)
	for rows.Next() {
		var item HDDashboardRow
		var bnbBalance string
		var usdtBalance string
		var updatedAt mysqlDriver.NullTime
		if err := rows.Scan(&item.WalletIndex, &item.MnemonicTag, &item.Address, &bnbBalance, &usdtBalance, &updatedAt); err != nil {
			return nil, 0, fmt.Errorf("scan hd bsc dashboard row: %w", err)
		}
		if updatedAt.Valid {
			item.UpdatedAt = sql.NullTime{Time: updatedAt.Time, Valid: true}
		}
		item.BNBBalance, err = decimal.NewFromString(bnbBalance)
		if err != nil {
			return nil, 0, fmt.Errorf("parse hd bsc bnb balance: %w", err)
		}
		item.USDTBalance, err = decimal.NewFromString(usdtBalance)
		if err != nil {
			return nil, 0, fmt.Errorf("parse hd bsc usdt balance: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return result, total, nil
}

func (d *DB) GetHDTronDashboardRowByAddress(ctx context.Context, source, address string) (*HDDashboardRow, bool, error) {
	source = normalizeHDSource(source)
	address = strings.TrimSpace(address)
	if address == "" {
		return nil, false, fmt.Errorf("address is required")
	}

	var item HDDashboardRow
	var trxBalance string
	var usdtBalance string
	var updatedAt sql.NullTime
	err := d.sql.QueryRowContext(ctx, `
		SELECT
			w.wallet_index,
			w.mnemonic_tag,
			w.address_base58,
			COALESCE(trx.balance, 0) AS trx_balance,
			COALESCE(usdt.balance, 0) AS usdt_balance,
			CASE
				WHEN trx.updated_at IS NULL AND usdt.updated_at IS NULL THEN w.updated_at
				WHEN trx.updated_at IS NULL THEN usdt.updated_at
				WHEN usdt.updated_at IS NULL THEN trx.updated_at
				WHEN trx.updated_at >= usdt.updated_at THEN trx.updated_at
				ELSE usdt.updated_at
			END AS updated_at
		FROM watch_addresses w
		LEFT JOIN asset_balances trx
			ON trx.address_base58 = w.address_base58
			AND trx.asset_code = 'TRX'
		LEFT JOIN asset_balances usdt
			ON usdt.address_base58 = w.address_base58
			AND usdt.asset_code = 'USDT'
		WHERE w.status = 1
		  AND w.source = ?
		  AND w.address_base58 = ?
		LIMIT 1
	`, source, address).Scan(&item.WalletIndex, &item.MnemonicTag, &item.Address, &trxBalance, &usdtBalance, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get hd tron dashboard row by address: %w", err)
	}
	item.UpdatedAt = normalizeNullTime(updatedAt)
	item.TRXBalance, err = decimal.NewFromString(trxBalance)
	if err != nil {
		return nil, false, fmt.Errorf("parse hd tron trx balance: %w", err)
	}
	item.USDTBalance, err = decimal.NewFromString(usdtBalance)
	if err != nil {
		return nil, false, fmt.Errorf("parse hd tron usdt balance: %w", err)
	}
	return &item, true, nil
}

func (d *DB) GetHDBSCDashboardRowByAddress(ctx context.Context, source, address string) (*HDDashboardRow, bool, error) {
	source = normalizeHDSource(source)
	address = strings.ToLower(strings.TrimSpace(address))
	if address == "" {
		return nil, false, fmt.Errorf("address is required")
	}

	var item HDDashboardRow
	var bnbBalance string
	var usdtBalance string
	var updatedAt mysqlDriver.NullTime
	err := d.sql.QueryRowContext(ctx, `
		SELECT
			w.wallet_index,
			w.mnemonic_tag,
			w.address,
			COALESCE(bnb.balance, 0) AS bnb_balance,
			COALESCE(usdt.balance, 0) AS usdt_balance,
			CASE
				WHEN bal.updated_at IS NOT NULL THEN bal.updated_at
				ELSE w.updated_at
			END AS updated_at
		FROM bsc_watch_addresses w
		LEFT JOIN (
			SELECT LOWER(address) AS address, MAX(updated_at) AS updated_at
			FROM bsc_asset_balances
			GROUP BY LOWER(address)
		) bal
			ON bal.address = LOWER(w.address)
		LEFT JOIN bsc_asset_balances bnb
			ON LOWER(bnb.address) = LOWER(w.address) AND bnb.asset_code = 'BNB'
		LEFT JOIN bsc_asset_balances usdt
			ON LOWER(usdt.address) = LOWER(w.address) AND usdt.asset_code = 'USDT'
		WHERE w.status = 1
		  AND w.source = ?
		  AND LOWER(w.address) = ?
		LIMIT 1
	`, source, address).Scan(&item.WalletIndex, &item.MnemonicTag, &item.Address, &bnbBalance, &usdtBalance, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get hd bsc dashboard row by address: %w", err)
	}
	if updatedAt.Valid {
		item.UpdatedAt = sql.NullTime{Time: updatedAt.Time, Valid: true}
	}
	item.BNBBalance, err = decimal.NewFromString(bnbBalance)
	if err != nil {
		return nil, false, fmt.Errorf("parse hd bsc bnb balance: %w", err)
	}
	item.USDTBalance, err = decimal.NewFromString(usdtBalance)
	if err != nil {
		return nil, false, fmt.Errorf("parse hd bsc usdt balance: %w", err)
	}
	return &item, true, nil
}

func (d *DB) GetHDTronSummary(ctx context.Context, source string) (HDSummary, error) {
	source = normalizeHDSource(source)
	var summary HDSummary
	var trxTotal string
	var usdtTotal string
	err := d.sql.QueryRowContext(ctx, `
		SELECT
			COUNT(1) AS total_count,
			COALESCE(SUM(CAST(COALESCE(trx.balance, 0) AS DECIMAL(36, 6))), 0) AS trx_total,
			COALESCE(SUM(CAST(COALESCE(usdt.balance, 0) AS DECIMAL(36, 6))), 0) AS usdt_total,
			MAX(
				CASE
					WHEN trx.updated_at IS NULL AND usdt.updated_at IS NULL THEN w.updated_at
					WHEN trx.updated_at IS NULL THEN usdt.updated_at
					WHEN usdt.updated_at IS NULL THEN trx.updated_at
					WHEN trx.updated_at >= usdt.updated_at THEN trx.updated_at
					ELSE usdt.updated_at
				END
			) AS last_updated_at
		FROM watch_addresses w
		LEFT JOIN asset_balances trx
			ON trx.address_base58 = w.address_base58
			AND trx.asset_code = 'TRX'
		LEFT JOIN asset_balances usdt
			ON usdt.address_base58 = w.address_base58
			AND usdt.asset_code = 'USDT'
		WHERE w.status = 1
		  AND w.source = ?
	`, source).Scan(&summary.Count, &trxTotal, &usdtTotal, &summary.LastUpdated)
	if err != nil {
		return HDSummary{}, fmt.Errorf("get hd tron summary: %w", err)
	}
	summary.TRXTotal, err = decimal.NewFromString(trxTotal)
	if err != nil {
		return HDSummary{}, fmt.Errorf("parse hd tron trx total: %w", err)
	}
	summary.USDTTotal, err = decimal.NewFromString(usdtTotal)
	if err != nil {
		return HDSummary{}, fmt.Errorf("parse hd tron usdt total: %w", err)
	}
	return summary, nil
}

func (d *DB) GetHDBSCSummary(ctx context.Context, source string) (HDSummary, error) {
	source = normalizeHDSource(source)
	var summary HDSummary
	var bnbTotal string
	var usdtTotal string
	var updatedAt mysqlDriver.NullTime
	err := d.sql.QueryRowContext(ctx, `
		SELECT
			COUNT(1) AS total_count,
			COALESCE(SUM(CAST(COALESCE(bnb.balance, 0) AS DECIMAL(36, 6))), 0) AS bnb_total,
			COALESCE(SUM(CAST(COALESCE(usdt.balance, 0) AS DECIMAL(36, 6))), 0) AS usdt_total,
			MAX(
				CASE
					WHEN bal.updated_at IS NOT NULL THEN bal.updated_at
					ELSE w.updated_at
				END
			) AS last_updated_at
		FROM bsc_watch_addresses w
		LEFT JOIN (
			SELECT LOWER(address) AS address, MAX(updated_at) AS updated_at
			FROM bsc_asset_balances
			GROUP BY LOWER(address)
		) bal
			ON bal.address = LOWER(w.address)
		LEFT JOIN bsc_asset_balances bnb
			ON LOWER(bnb.address) = LOWER(w.address) AND bnb.asset_code = 'BNB'
		LEFT JOIN bsc_asset_balances usdt
			ON LOWER(usdt.address) = LOWER(w.address) AND usdt.asset_code = 'USDT'
		WHERE w.status = 1
		  AND w.source = ?
	`, source).Scan(&summary.Count, &bnbTotal, &usdtTotal, &updatedAt)
	if err != nil {
		return HDSummary{}, fmt.Errorf("get hd bsc summary: %w", err)
	}
	if updatedAt.Valid {
		summary.LastUpdated = sql.NullTime{Time: updatedAt.Time, Valid: true}
	}
	summary.BNBTotal, err = decimal.NewFromString(bnbTotal)
	if err != nil {
		return HDSummary{}, fmt.Errorf("parse hd bsc bnb total: %w", err)
	}
	summary.USDTTotal, err = decimal.NewFromString(usdtTotal)
	if err != nil {
		return HDSummary{}, fmt.Errorf("parse hd bsc usdt total: %w", err)
	}
	return summary, nil
}

func (d *DB) countWatchAddressesBySource(ctx context.Context, table, source string) (int, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(1)
		FROM %s
		WHERE status = 1
		  AND source = ?
	`, table)
	var total int
	if err := d.sql.QueryRowContext(ctx, query, source).Scan(&total); err != nil {
		return 0, fmt.Errorf("count watch addresses by source: %w", err)
	}
	return total, nil
}

func normalizeHDSource(source string) string {
	trimmed := strings.TrimSpace(source)
	if trimmed == "" {
		return HDWalletSource
	}
	return trimmed
}

func (d *DB) GetHDTronNextWalletIndex(ctx context.Context, source, mnemonicTag string) (int, error) {
	return d.getHDNextWalletIndex(ctx, "watch_addresses", "address_base58", source, mnemonicTag)
}

func (d *DB) GetHDBSCNextWalletIndex(ctx context.Context, source, mnemonicTag string) (int, error) {
	return d.getHDNextWalletIndex(ctx, "bsc_watch_addresses", "address", source, mnemonicTag)
}

func (d *DB) ListHDTronWalletIndexes(ctx context.Context, source, mnemonicTag string) ([]int, error) {
	return d.listHDWalletIndexes(ctx, "watch_addresses", "address_base58", source, mnemonicTag)
}

func (d *DB) ListHDBSCWalletIndexes(ctx context.Context, source, mnemonicTag string) ([]int, error) {
	return d.listHDWalletIndexes(ctx, "bsc_watch_addresses", "address", source, mnemonicTag)
}

func (d *DB) getHDNextWalletIndex(ctx context.Context, table, addressColumn, source, mnemonicTag string) (int, error) {
	query := fmt.Sprintf(`
		SELECT COALESCE(MAX(wallet_index), -1)
		FROM %s
		WHERE status = 1
		  AND source = ?
		  AND mnemonic_tag = ?
		  AND %s <> ''
	`, table, addressColumn)
	var maxIndex int
	if err := d.sql.QueryRowContext(ctx, query, normalizeHDSource(source), strings.TrimSpace(mnemonicTag)).Scan(&maxIndex); err != nil {
		return 0, fmt.Errorf("get hd next wallet index: %w", err)
	}
	return maxIndex + 1, nil
}

func (d *DB) listHDWalletIndexes(ctx context.Context, table, addressColumn, source, mnemonicTag string) ([]int, error) {
	query := fmt.Sprintf(`
		SELECT wallet_index
		FROM %s
		WHERE status = 1
		  AND source = ?
		  AND mnemonic_tag = ?
		  AND %s <> ''
		ORDER BY wallet_index ASC
	`, table, addressColumn)
	rows, err := d.sql.QueryContext(ctx, query, normalizeHDSource(source), strings.TrimSpace(mnemonicTag))
	if err != nil {
		return nil, fmt.Errorf("list hd wallet indexes: %w", err)
	}
	defer rows.Close()

	indexes := make([]int, 0)
	for rows.Next() {
		var walletIndex int
		if err := rows.Scan(&walletIndex); err != nil {
			return nil, fmt.Errorf("scan hd wallet index: %w", err)
		}
		indexes = append(indexes, walletIndex)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate hd wallet indexes: %w", err)
	}
	return indexes, nil
}
