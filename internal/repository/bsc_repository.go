package repository

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"
)

type bscSQLExecutor interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type BSCDashboardRecord struct {
	Address   string
	BNB       string
	USDT      string
	UpdatedAt time.Time
}

type BSCDashboardSummary struct {
	TotalCount  int
	BNBTotal    decimal.Decimal
	USDTTotal   decimal.Decimal
	LastUpdated mysqlDriver.NullTime
}

type BSCDashboardSort string

const (
	BSCDashboardSortUSDTDesc BSCDashboardSort = "usdt_desc"
	BSCDashboardSortUSDTAsc  BSCDashboardSort = "usdt_asc"
	BSCDashboardSortBNBDesc  BSCDashboardSort = "bnb_desc"
	BSCDashboardSortBNBAsc   BSCDashboardSort = "bnb_asc"
)

func CountActiveBSCWatchAddresses(ctx context.Context, repo any) (int, error) {
	return CountActiveBSCWatchAddressesByQuery(ctx, repo, "")
}

func CountActiveBSCWatchAddressesByQuery(ctx context.Context, repo any, addressQuery string) (int, error) {
	executor, err := resolveBSCExecutor(repo)
	if err != nil {
		return 0, err
	}

	query := `
SELECT COUNT(1)
FROM bsc_watch_addresses
WHERE status = 1
`
	args := make([]any, 0, 1)
	if value := strings.ToLower(strings.TrimSpace(addressQuery)); value != "" {
		query += "  AND LOWER(address) LIKE ?\n"
		args = append(args, "%"+value+"%")
	}

	var total int
	if err := executor.QueryRowContext(ctx, query, args...).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func GetBSCDashboardSummary(ctx context.Context, repo any) (BSCDashboardSummary, error) {
	executor, err := resolveBSCExecutor(repo)
	if err != nil {
		return BSCDashboardSummary{}, err
	}

	var summary BSCDashboardSummary
	var bnbTotal string
	var usdtTotal string
	err = executor.QueryRowContext(ctx, `
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
FROM (
	SELECT id, address, updated_at
	FROM bsc_watch_addresses
	WHERE status = 1
) w
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
`).Scan(&summary.TotalCount, &bnbTotal, &usdtTotal, &summary.LastUpdated)
	if err != nil {
		return BSCDashboardSummary{}, err
	}

	summary.BNBTotal, err = decimal.NewFromString(bnbTotal)
	if err != nil {
		return BSCDashboardSummary{}, fmt.Errorf("parse bsc dashboard bnb total: %w", err)
	}
	summary.USDTTotal, err = decimal.NewFromString(usdtTotal)
	if err != nil {
		return BSCDashboardSummary{}, fmt.Errorf("parse bsc dashboard usdt total: %w", err)
	}
	return summary, nil
}

func GetBSCDashboardRecordByAddress(ctx context.Context, repo any, address string) (*BSCDashboardRecord, bool, error) {
	executor, err := resolveBSCExecutor(repo)
	if err != nil {
		return nil, false, err
	}

	address = strings.ToLower(strings.TrimSpace(address))
	if address == "" {
		return nil, false, fmt.Errorf("empty address")
	}

	query := `
SELECT
	w.address,
	COALESCE(bnb.balance, 0) AS bnb_balance,
	COALESCE(usdt.balance, 0) AS usdt_balance,
	CASE
		WHEN bal.updated_at IS NOT NULL THEN bal.updated_at
		ELSE w.updated_at
	END AS updated_at
FROM (
	SELECT id, address, updated_at
	FROM bsc_watch_addresses
	WHERE status = 1
) w
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
WHERE LOWER(w.address) = ?
LIMIT 1
`

	var record BSCDashboardRecord
	var bnbBalance string
	var usdtBalance string
	var updatedAt mysqlDriver.NullTime
	if err := executor.QueryRowContext(ctx, query, address).Scan(&record.Address, &bnbBalance, &usdtBalance, &updatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, err
	}

	record.BNB = bnbBalance
	record.USDT = usdtBalance
	if updatedAt.Valid {
		record.UpdatedAt = updatedAt.Time
	}
	return &record, true, nil
}

func ListBSCDashboardRecords(ctx context.Context, repo any, limit, offset int, sort BSCDashboardSort) ([]BSCDashboardRecord, error) {
	return ListBSCDashboardRecordsByQuery(ctx, repo, limit, offset, sort, "")
}

func ListBSCDashboardRecordsByQuery(ctx context.Context, repo any, limit, offset int, sort BSCDashboardSort, addressQuery string) ([]BSCDashboardRecord, error) {
	executor, err := resolveBSCExecutor(repo)
	if err != nil {
		return nil, err
	}

	orderBy := "COALESCE(usdt.balance, 0) DESC, w.id DESC"
	switch sort {
	case BSCDashboardSortUSDTAsc:
		orderBy = "COALESCE(usdt.balance, 0) ASC, w.id DESC"
	case BSCDashboardSortBNBDesc:
		orderBy = "COALESCE(bnb.balance, 0) DESC, w.id DESC"
	case BSCDashboardSortBNBAsc:
		orderBy = "COALESCE(bnb.balance, 0) ASC, w.id DESC"
	default:
		sort = BSCDashboardSortUSDTDesc
	}

	where := ""
	args := make([]any, 0, 3)
	if value := strings.ToLower(strings.TrimSpace(addressQuery)); value != "" {
		where = "WHERE LOWER(w.address) LIKE ?"
		args = append(args, "%"+value+"%")
	}

	query := fmt.Sprintf(`
SELECT
	w.address,
	COALESCE(bnb.balance, 0) AS bnb_balance,
	COALESCE(usdt.balance, 0) AS usdt_balance,
	CASE
		WHEN bal.updated_at IS NOT NULL THEN bal.updated_at
		ELSE w.updated_at
	END AS updated_at
FROM (
	SELECT id, address, updated_at
	FROM bsc_watch_addresses
	WHERE status = 1
) w
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
%s
ORDER BY %s
LIMIT ? OFFSET ?
`, where, orderBy)

	args = append(args, limit, offset)

	rows, err := executor.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]BSCDashboardRecord, 0, limit)
	for rows.Next() {
		var record BSCDashboardRecord
		var bnbBalance string
		var usdtBalance string
		var updatedAt mysqlDriver.NullTime
		if err := rows.Scan(&record.Address, &bnbBalance, &usdtBalance, &updatedAt); err != nil {
			return nil, err
		}
		record.BNB = bnbBalance
		record.USDT = usdtBalance
		if updatedAt.Valid {
			record.UpdatedAt = updatedAt.Time
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func FindExistingBSCWatchAddresses(ctx context.Context, repo any, addresses []string) (map[string]struct{}, error) {
	executor, err := resolveBSCExecutor(repo)
	if err != nil {
		return nil, err
	}

	result := make(map[string]struct{})
	if len(addresses) == 0 {
		return result, nil
	}

	placeholders := make([]string, 0, len(addresses))
	args := make([]any, 0, len(addresses))
	for _, address := range addresses {
		address = strings.TrimSpace(address)
		if address == "" {
			continue
		}
		placeholders = append(placeholders, "?")
		args = append(args, address)
	}
	if len(placeholders) == 0 {
		return result, nil
	}

	rows, err := executor.QueryContext(ctx, fmt.Sprintf(`
		SELECT LOWER(address)
		FROM bsc_watch_addresses
		WHERE status = 1
		  AND LOWER(address) IN (%s)
	`, strings.Join(placeholders, ",")), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var address string
		if err := rows.Scan(&address); err != nil {
			return nil, err
		}
		result[address] = struct{}{}
	}
	return result, rows.Err()
}

func InsertBSCWatchAddresses(ctx context.Context, repo any, addresses []string) error {
	executor, err := resolveBSCExecutor(repo)
	if err != nil {
		return err
	}
	if len(addresses) == 0 {
		return nil
	}

	tx, err := beginBSCTx(ctx, repo, executor)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO bsc_watch_addresses (address, status)
		VALUES (?, 1)
		ON DUPLICATE KEY UPDATE
			status = 1,
			updated_at = CURRENT_TIMESTAMP
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, address := range addresses {
		address = strings.ToLower(strings.TrimSpace(address))
		if address == "" {
			continue
		}
		if _, err := stmt.ExecContext(ctx, address); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func LoadActiveBSCWatchAddresses(ctx context.Context, repo any) ([]string, error) {
	executor, err := resolveBSCExecutor(repo)
	if err != nil {
		return nil, err
	}

	rows, err := executor.QueryContext(ctx, `
		SELECT DISTINCT LOWER(address)
		FROM bsc_watch_addresses
		WHERE status = 1
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]string, 0)
	for rows.Next() {
		var address string
		if err := rows.Scan(&address); err != nil {
			return nil, err
		}
		address = strings.ToLower(strings.TrimSpace(address))
		if address == "" {
			continue
		}
		result = append(result, address)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func LoadActiveBSCWatchAddressesWithPositiveBNBBalance(ctx context.Context, repo any) ([]string, error) {
	executor, err := resolveBSCExecutor(repo)
	if err != nil {
		return nil, err
	}

	rows, err := executor.QueryContext(ctx, `
		SELECT DISTINCT LOWER(w.address)
		FROM bsc_watch_addresses w
		INNER JOIN bsc_asset_balances bnb
			ON LOWER(bnb.address) = LOWER(w.address)
			AND bnb.asset_code = 'BNB'
		WHERE w.status = 1
		  AND bnb.balance > 0
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]string, 0)
	for rows.Next() {
		var address string
		if err := rows.Scan(&address); err != nil {
			return nil, err
		}
		address = strings.ToLower(strings.TrimSpace(address))
		if address == "" {
			continue
		}
		result = append(result, address)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func LoadActiveBSCWatchAddressesBySource(ctx context.Context, repo any, source string) ([]string, error) {
	executor, err := resolveBSCExecutor(repo)
	if err != nil {
		return nil, err
	}

	rows, err := executor.QueryContext(ctx, `
		SELECT DISTINCT LOWER(address)
		FROM bsc_watch_addresses
		WHERE status = 1
		  AND source = ?
	`, strings.TrimSpace(source))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]string, 0)
	for rows.Next() {
		var address string
		if err := rows.Scan(&address); err != nil {
			return nil, err
		}
		address = strings.ToLower(strings.TrimSpace(address))
		if address == "" {
			continue
		}
		result = append(result, address)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func UpsertBSCBalance(ctx context.Context, repo any, address string, assetCode string, balance string) error {
	executor, err := resolveBSCExecutor(repo)
	if err != nil {
		return err
	}
	address = strings.ToLower(strings.TrimSpace(address))
	assetCode = strings.ToUpper(strings.TrimSpace(assetCode))
	if address == "" || assetCode == "" {
		return fmt.Errorf("address and asset_code are required")
	}

	_, err = executor.ExecContext(ctx, `
		INSERT INTO bsc_asset_balances (address, asset_code, balance)
		VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE
			balance = VALUES(balance),
			updated_at = CURRENT_TIMESTAMP
	`, address, assetCode, strings.TrimSpace(balance))
	return err
}

func SoftDeleteBSCWatchAddresses(ctx context.Context, repo any, addresses []string) (int64, error) {
	executor, err := resolveBSCExecutor(repo)
	if err != nil {
		return 0, err
	}

	if len(addresses) == 0 {
		return 0, nil
	}

	uniqueAddresses := make([]string, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		address = strings.ToLower(strings.TrimSpace(address))
		if address == "" {
			continue
		}
		if _, ok := seen[address]; ok {
			continue
		}
		seen[address] = struct{}{}
		uniqueAddresses = append(uniqueAddresses, address)
	}
	if len(uniqueAddresses) == 0 {
		return 0, nil
	}

	placeholders := make([]string, len(uniqueAddresses))
	args := make([]interface{}, 0, len(uniqueAddresses))
	for i, address := range uniqueAddresses {
		placeholders[i] = "?"
		args = append(args, address)
	}

	query := fmt.Sprintf(
		`UPDATE bsc_watch_addresses
SET status = 0, updated_at = CURRENT_TIMESTAMP
WHERE status = 1 AND LOWER(address) IN (%s)`,
		strings.Join(placeholders, ","),
	)

	result, err := executor.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func beginBSCTx(ctx context.Context, repo any, executor bscSQLExecutor) (*sql.Tx, error) {
	if db, ok := repo.(*DB); ok && db != nil && db.sql != nil {
		return db.sql.BeginTx(ctx, nil)
	}
	if sqlDB, ok := repo.(*sql.DB); ok && sqlDB != nil {
		return sqlDB.BeginTx(ctx, nil)
	}
	if tx, ok := executor.(*sql.Tx); ok && tx != nil {
		return tx, nil
	}
	return nil, fmt.Errorf("begin tx failed")
}

func resolveBSCExecutor(repo any) (bscSQLExecutor, error) {
	if repo == nil {
		return nil, fmt.Errorf("repository is nil")
	}

	if executor, ok := repo.(bscSQLExecutor); ok && executor != nil {
		return executor, nil
	}

	value := reflect.ValueOf(repo)
	executor, ok := findBSCExecutor(value, 0)
	if ok {
		return executor, nil
	}

	return nil, fmt.Errorf("sql executor not found on repository")
}

func findBSCExecutor(value reflect.Value, depth int) (bscSQLExecutor, bool) {
	if depth > 4 || !value.IsValid() {
		return nil, false
	}

	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, false
		}
		if value.CanInterface() {
			if executor, ok := value.Interface().(bscSQLExecutor); ok && executor != nil {
				return executor, true
			}
		}
		return findBSCExecutor(value.Elem(), depth+1)
	}

	if value.CanInterface() {
		if executor, ok := value.Interface().(bscSQLExecutor); ok && executor != nil {
			return executor, true
		}
	}

	if value.Kind() != reflect.Struct {
		return nil, false
	}

	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		if !field.IsValid() {
			continue
		}
		if field.Kind() == reflect.Pointer && field.IsNil() {
			continue
		}
		if executor, ok := findBSCExecutor(field, depth+1); ok {
			return executor, true
		}
	}

	return nil, false
}
