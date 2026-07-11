package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

type WatchAddress struct {
	ID            int64
	AddressBase58 string
	Status        int
}

type TransferRecord struct {
	TxHash          string
	BlockNumber     int64
	BlockTime       int64
	AssetCode       string
	ContractAddress sql.NullString
	WatchAddress    string
	FromAddress     string
	ToAddress       string
	Amount          decimal.Decimal
	Direction       string
	LogIndex        int
	Status          string
}

type DB struct {
	sql *sql.DB
}

type DashboardRow struct {
	AddressBase58 string
	TRXBalance    decimal.Decimal
	USDTBalance   decimal.Decimal
	LastUpdatedAt sql.NullTime
}

type DashboardListResult struct {
	Rows       []DashboardRow
	TotalCount int
}

func (d *DB) GetDashboardRowByAddress(ctx context.Context, addressBase58 string) (*DashboardRow, bool, error) {
	var (
		row         DashboardRow
		trxBalance  string
		usdtBalance string
		lastUpdated sql.NullTime
	)

	err := d.sql.QueryRowContext(ctx, `
		SELECT
			w.address_base58,
			COALESCE(trx.balance, 0) AS trx_balance,
			COALESCE(usdt.balance, 0) AS usdt_balance,
			CASE
				WHEN trx.updated_at IS NULL AND usdt.updated_at IS NULL THEN NULL
				WHEN trx.updated_at IS NULL THEN usdt.updated_at
				WHEN usdt.updated_at IS NULL THEN trx.updated_at
				WHEN trx.updated_at >= usdt.updated_at THEN trx.updated_at
				ELSE usdt.updated_at
			END AS last_updated_at
		FROM watch_addresses w
		LEFT JOIN asset_balances trx
			ON trx.address_base58 = w.address_base58
			AND trx.asset_code = 'TRX'
		LEFT JOIN asset_balances usdt
			ON usdt.address_base58 = w.address_base58
			AND usdt.asset_code = 'USDT'
		WHERE w.status = 1
		  AND w.address_base58 = ?
		LIMIT 1
	`, addressBase58).Scan(&row.AddressBase58, &trxBalance, &usdtBalance, &lastUpdated)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get dashboard row by address: %w", err)
	}
	row.LastUpdatedAt = normalizeNullTime(lastUpdated)

	value, err := decimal.NewFromString(trxBalance)
	if err != nil {
		return nil, false, fmt.Errorf("parse trx balance: %w", err)
	}
	row.TRXBalance = value

	value, err = decimal.NewFromString(usdtBalance)
	if err != nil {
		return nil, false, fmt.Errorf("parse usdt balance: %w", err)
	}
	row.USDTBalance = value

	return &row, true, nil
}

type TransferListRecord struct {
	TxHash          string
	BlockNumber     int64
	BlockTime       int64
	AssetCode       string
	ContractAddress sql.NullString
	WatchAddress    string
	FromAddress     string
	ToAddress       string
	Amount          decimal.Decimal
	LogIndex        int
	Status          string
	CreatedAt       time.Time
}

type TransferListResult struct {
	Records    []TransferListRecord
	TotalCount int
}

type EnergyActionLog struct {
	ActionName    string
	AddressBase58 string
	Provider      string
	EnergyAmount  int
	ActionScore   int
	Status        string
	ResponseBody  string
	ErrorMessage  string
}

type TronActivationLog struct {
	JobID             string
	AddressBase58     string
	FromAddressBase58 string
	AmountSun         int64
	TxID              string
	Status            string
	ErrorMessage      string
}

type BSCGasTopupLog struct {
	Address      string
	FromAddress  string
	AmountBNB    string
	CurrentBNB   string
	CurrentUSDT  string
	TxHash       string
	KeySource    string
	Status       string
	ResponseBody string
	ErrorMessage string
}

type EnergyChartPoint struct {
	Day   string
	Count int
}

type DashboardSort string

const (
	DashboardSortUSDTDesc DashboardSort = "usdt_desc"
	DashboardSortUSDTAsc  DashboardSort = "usdt_asc"
	DashboardSortTRXDesc  DashboardSort = "trx_desc"
	DashboardSortTRXAsc   DashboardSort = "trx_asc"
)

func New(db *sql.DB) *DB {
	return &DB{sql: db}
}

func (d *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return d.sql.QueryRowContext(ctx, query, args...)
}

func (d *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return d.sql.QueryContext(ctx, query, args...)
}

func (d *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return d.sql.ExecContext(ctx, query, args...)
}

func (d *DB) LoadActiveAddresses(ctx context.Context) ([]WatchAddress, error) {
	rows, err := d.sql.QueryContext(ctx, `
		SELECT id, address_base58, status
		FROM watch_addresses
		WHERE status = 1 
	`)
	if err != nil {
		return nil, fmt.Errorf("query watch_addresses: %w", err)
	}
	defer rows.Close()

	result := make([]WatchAddress, 0)
	for rows.Next() {
		var item WatchAddress
		if err := rows.Scan(&item.ID, &item.AddressBase58, &item.Status); err != nil {
			return nil, fmt.Errorf("scan watch_address: %w", err)
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (d *DB) LoadActiveAddressesBySource(ctx context.Context, source string) ([]WatchAddress, error) {
	rows, err := d.sql.QueryContext(ctx, `
		SELECT id, address_base58, status
		FROM watch_addresses
		WHERE status = 1
		  AND source = ?
	`, strings.TrimSpace(source))
	if err != nil {
		return nil, fmt.Errorf("query watch_addresses by source: %w", err)
	}
	defer rows.Close()

	result := make([]WatchAddress, 0)
	for rows.Next() {
		var item WatchAddress
		if err := rows.Scan(&item.ID, &item.AddressBase58, &item.Status); err != nil {
			return nil, fmt.Errorf("scan watch_address by source: %w", err)
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (d *DB) GetLastBlock(ctx context.Context, syncKey string) (int64, bool, error) {
	var lastBlock int64
	err := d.sql.QueryRowContext(ctx, `
		SELECT last_block
		FROM sync_state
		WHERE sync_key = ?
	`, syncKey).Scan(&lastBlock)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("get last_block: %w", err)
	}
	return lastBlock, true, nil
}

func (d *DB) SaveLastBlock(ctx context.Context, syncKey string, block int64) error {
	_, err := d.sql.ExecContext(ctx, `
		INSERT INTO sync_state (sync_key, last_block)
		VALUES (?, ?)
		ON DUPLICATE KEY UPDATE last_block = VALUES(last_block)
	`, syncKey, block)
	if err != nil {
		return fmt.Errorf("save last_block: %w", err)
	}
	return nil
}

func (d *DB) UpsertBalance(ctx context.Context, addressBase58, assetCode string, balance decimal.Decimal, blockNumber int64) error {
	_, err := d.sql.ExecContext(ctx, `
		INSERT INTO asset_balances (address_base58, asset_code, balance, block_number)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			balance = VALUES(balance),
			block_number = VALUES(block_number),
			updated_at = CURRENT_TIMESTAMP
	`, addressBase58, assetCode, balance.String(), blockNumber)
	if err != nil {
		return fmt.Errorf("upsert balance: %w", err)
	}
	return nil
}

func (d *DB) InsertTransfer(ctx context.Context, item TransferRecord) error {
	_, err := d.sql.ExecContext(ctx, `
		INSERT INTO transfer_records (
			tx_hash, block_number, block_time, asset_code, contract_address,
			from_address, to_address, amount, direction, log_index, status
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			direction = VALUES(direction),
			status = VALUES(status)
	`, item.TxHash, item.BlockNumber, item.BlockTime, item.AssetCode, item.ContractAddress,
		item.FromAddress, item.ToAddress, item.Amount.String(), item.Direction, item.LogIndex, item.Status)
	if err != nil {
		return fmt.Errorf("insert transfer: %w", err)
	}
	return nil
}

func (d *DB) InsertTransferIn(ctx context.Context, item TransferRecord) error {
	return d.insertTransferIntoTable(ctx, "transfer_in_records", item)
}

func (d *DB) InsertTransferOut(ctx context.Context, item TransferRecord) error {
	return d.insertTransferIntoTable(ctx, "transfer_out_records", item)
}

func (d *DB) InsertBSCTransferIn(ctx context.Context, item TransferRecord) error {
	return d.insertTransferIntoTable(ctx, "bsc_transfer_in_records", item)
}

func (d *DB) InsertBSCTransferOut(ctx context.Context, item TransferRecord) error {
	return d.insertTransferIntoTable(ctx, "bsc_transfer_out_records", item)
}

func (d *DB) InsertTransferInIfAbsent(ctx context.Context, item TransferRecord) (bool, error) {
	return d.insertTransferIntoTableIfAbsent(ctx, "transfer_in_records", item)
}

func (d *DB) InsertTransferOutIfAbsent(ctx context.Context, item TransferRecord) (bool, error) {
	return d.insertTransferIntoTableIfAbsent(ctx, "transfer_out_records", item)
}

func (d *DB) InsertBSCTransferInIfAbsent(ctx context.Context, item TransferRecord) (bool, error) {
	return d.insertTransferIntoTableIfAbsent(ctx, "bsc_transfer_in_records", item)
}

func (d *DB) InsertBSCTransferOutIfAbsent(ctx context.Context, item TransferRecord) (bool, error) {
	return d.insertTransferIntoTableIfAbsent(ctx, "bsc_transfer_out_records", item)
}

func (d *DB) insertTransferIntoTable(ctx context.Context, table string, item TransferRecord) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (
			tx_hash, block_number, block_time, asset_code, contract_address,
			watch_address, from_address, to_address, amount, log_index, status
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			block_number = VALUES(block_number),
			block_time = VALUES(block_time),
			asset_code = VALUES(asset_code),
			contract_address = VALUES(contract_address),
			watch_address = VALUES(watch_address),
			from_address = VALUES(from_address),
			to_address = VALUES(to_address),
			amount = VALUES(amount),
			log_index = VALUES(log_index),
			status = VALUES(status)
	`, table)

	_, err := d.sql.ExecContext(ctx, query,
		item.TxHash,
		item.BlockNumber,
		item.BlockTime,
		item.AssetCode,
		item.ContractAddress,
		item.WatchAddress,
		item.FromAddress,
		item.ToAddress,
		item.Amount.String(),
		item.LogIndex,
		item.Status,
	)
	if err != nil {
		return fmt.Errorf("insert transfer %s: %w", table, err)
	}
	return nil
}

func (d *DB) insertTransferIntoTableIfAbsent(ctx context.Context, table string, item TransferRecord) (bool, error) {
	query := fmt.Sprintf(`
		INSERT IGNORE INTO %s (
			tx_hash, block_number, block_time, asset_code, contract_address,
			watch_address, from_address, to_address, amount, log_index, status
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, table)

	result, err := d.sql.ExecContext(ctx, query,
		item.TxHash,
		item.BlockNumber,
		item.BlockTime,
		item.AssetCode,
		item.ContractAddress,
		item.WatchAddress,
		item.FromAddress,
		item.ToAddress,
		item.Amount.String(),
		item.LogIndex,
		item.Status,
	)
	if err != nil {
		return false, fmt.Errorf("insert transfer if absent %s: %w", table, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("insert transfer if absent rows affected %s: %w", table, err)
	}
	return rowsAffected > 0, nil
}

func (d *DB) InsertWatchAddresses(ctx context.Context, addresses []string) error {
	if len(addresses) == 0 {
		return nil
	}

	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx for watch_addresses: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO watch_addresses (address_base58, status)
		VALUES (?, 1)
		ON DUPLICATE KEY UPDATE
			status = 1,
			updated_at = CURRENT_TIMESTAMP
	`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare insert watch_addresses: %w", err)
	}
	defer stmt.Close()

	for _, address := range addresses {
		if _, err := stmt.ExecContext(ctx, address); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert watch_address %s: %w", address, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit watch_addresses: %w", err)
	}
	return nil
}

func (d *DB) DisableWatchAddress(ctx context.Context, address string) error {
	_, err := d.sql.ExecContext(ctx, `
		UPDATE watch_addresses
		SET status = 0,
			updated_at = CURRENT_TIMESTAMP
		WHERE address_base58 = ?
	`, address)
	if err != nil {
		return fmt.Errorf("disable watch address %s: %w", address, err)
	}
	return nil
}

func (d *DB) FindExistingWatchAddresses(ctx context.Context, addresses []string) (map[string]struct{}, error) {
	result := make(map[string]struct{})
	if len(addresses) == 0 {
		return result, nil
	}

	placeholders := make([]string, 0, len(addresses))
	args := make([]any, 0, len(addresses))
	for _, address := range addresses {
		placeholders = append(placeholders, "?")
		args = append(args, address)
	}

	rows, err := d.sql.QueryContext(ctx, fmt.Sprintf(`
		SELECT address_base58
		FROM watch_addresses
		WHERE status = 1
		  AND address_base58 IN (%s)
	`, strings.Join(placeholders, ",")), args...)
	if err != nil {
		return nil, fmt.Errorf("find existing watch_addresses: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var address string
		if err := rows.Scan(&address); err != nil {
			return nil, fmt.Errorf("scan existing watch_address: %w", err)
		}
		result[address] = struct{}{}
	}
	return result, rows.Err()
}

func (d *DB) GetRuntimeSetting(ctx context.Context, key string) (string, bool, error) {
	var value string
	err := d.sql.QueryRowContext(ctx, `
		SELECT setting_value
		FROM runtime_settings
		WHERE setting_key = ?
	`, key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("get runtime setting %s: %w", key, err)
	}
	return value, true, nil
}

func (d *DB) UpsertRuntimeSetting(ctx context.Context, key, value string) error {
	_, err := d.sql.ExecContext(ctx, `
		INSERT INTO runtime_settings (setting_key, setting_value)
		VALUES (?, ?)
		ON DUPLICATE KEY UPDATE
			setting_value = VALUES(setting_value),
			updated_at = CURRENT_TIMESTAMP
	`, key, value)
	if err != nil {
		return fmt.Errorf("upsert runtime setting %s: %w", key, err)
	}
	return nil
}

func (d *DB) InsertEnergyActionLog(ctx context.Context, item EnergyActionLog) error {
	_, err := d.sql.ExecContext(ctx, `
		INSERT INTO energy_action_logs (
			action_name, address_base58, provider, energy_amount, action_score,
			status, response_body, error_message
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, item.ActionName, item.AddressBase58, item.Provider, item.EnergyAmount, item.ActionScore,
		item.Status, item.ResponseBody, item.ErrorMessage)
	if err != nil {
		return fmt.Errorf("insert energy action log: %w", err)
	}
	return nil
}

func (d *DB) InsertTronActivationLog(ctx context.Context, item TronActivationLog) error {
	_, err := d.sql.ExecContext(ctx, `
		INSERT INTO tron_activation_logs (
			job_id, address_base58, from_address_base58, amount_sun,
			txid, status, error_message
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, item.JobID, item.AddressBase58, item.FromAddressBase58, item.AmountSun,
		item.TxID, item.Status, item.ErrorMessage)
	if err != nil {
		return fmt.Errorf("insert tron activation log: %w", err)
	}
	return nil
}

func (d *DB) InsertBSCGasTopupLog(ctx context.Context, item BSCGasTopupLog) error {
	_, err := d.sql.ExecContext(ctx, `
		INSERT INTO bsc_gas_topup_logs (
			address, from_address, amount_bnb, current_bnb, current_usdt,
			tx_hash, key_source, status, response_body, error_message
		) VALUES (?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, ?)
	`, item.Address, item.FromAddress, item.AmountBNB, item.CurrentBNB, item.CurrentUSDT,
		item.TxHash, item.KeySource, item.Status, item.ResponseBody, item.ErrorMessage)
	if err != nil {
		return fmt.Errorf("insert bsc gas topup log: %w", err)
	}
	return nil
}

func (d *DB) ListDailyEnergyChart(ctx context.Context, days int) ([]EnergyChartPoint, error) {
	if days <= 0 {
		days = 30
	}

	rows, err := d.sql.QueryContext(ctx, `
		SELECT DATE_FORMAT(created_at, '%Y-%m-%d') AS day_key, COALESCE(SUM(action_score), 0) AS total_score
		FROM energy_action_logs
		WHERE status = 'SUCCESS'
		  AND created_at >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL ? DAY)
		GROUP BY day_key
		ORDER BY day_key ASC
	`, days)
	if err != nil {
		return nil, fmt.Errorf("list daily energy chart: %w", err)
	}
	defer rows.Close()

	pointsByDay := make(map[string]int)
	for rows.Next() {
		var point EnergyChartPoint
		if err := rows.Scan(&point.Day, &point.Count); err != nil {
			return nil, fmt.Errorf("scan energy chart row: %w", err)
		}
		pointsByDay[point.Day] = point.Count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	loc := time.FixedZone("CST", 8*3600)
	today := time.Now().In(loc)
	result := make([]EnergyChartPoint, 0, days)
	for i := days - 1; i >= 0; i-- {
		day := today.AddDate(0, 0, -i).Format("2006-01-02")
		result = append(result, EnergyChartPoint{
			Day:   day,
			Count: pointsByDay[day],
		})
	}
	return result, nil
}

func (d *DB) ListDailyTronActivationChart(ctx context.Context, days int) ([]EnergyChartPoint, error) {
	if days <= 0 {
		days = 30
	}

	rows, err := d.sql.QueryContext(ctx, `
		SELECT DATE_FORMAT(created_at, '%Y-%m-%d') AS day_key, COUNT(1) AS total_count
		FROM tron_activation_logs
		WHERE status = 'SUCCESS'
		  AND created_at >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL ? DAY)
		GROUP BY day_key
		ORDER BY day_key ASC
	`, days)
	if err != nil {
		return nil, fmt.Errorf("list daily tron activation chart: %w", err)
	}
	defer rows.Close()

	pointsByDay := make(map[string]int)
	for rows.Next() {
		var point EnergyChartPoint
		if err := rows.Scan(&point.Day, &point.Count); err != nil {
			return nil, fmt.Errorf("scan tron activation chart row: %w", err)
		}
		pointsByDay[point.Day] = point.Count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	loc := time.FixedZone("CST", 8*3600)
	today := time.Now().In(loc)
	result := make([]EnergyChartPoint, 0, days)
	for i := days - 1; i >= 0; i-- {
		day := today.AddDate(0, 0, -i).Format("2006-01-02")
		result = append(result, EnergyChartPoint{
			Day:   day,
			Count: pointsByDay[day],
		})
	}
	return result, nil
}

func (d *DB) ListDashboardRows(ctx context.Context, offset, limit int, sort DashboardSort) (*DashboardListResult, error) {
	return d.ListDashboardRowsByAddress(ctx, offset, limit, sort, "")
}

func (d *DB) ListDashboardRowsByAddress(ctx context.Context, offset, limit int, sort DashboardSort, addressQuery string) (*DashboardListResult, error) {
	where := "WHERE w.status = 1"
	countWhere := "WHERE status = 1"
	args := make([]any, 0, 3)
	if value := strings.ToLower(strings.TrimSpace(addressQuery)); value != "" {
		where += " AND LOWER(w.address_base58) LIKE ?"
		countWhere += " AND LOWER(address_base58) LIKE ?"
		args = append(args, "%"+value+"%")
	}

	var totalCount int
	err := d.sql.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM watch_addresses
		`+countWhere, args...).Scan(&totalCount)
	if err != nil {
		return nil, fmt.Errorf("count dashboard rows: %w", err)
	}

	orderBy := "COALESCE(usdt.balance, 0) DESC, w.id DESC"
	switch sort {
	case DashboardSortUSDTAsc:
		orderBy = "COALESCE(usdt.balance, 0) ASC, w.id DESC"
	case DashboardSortTRXDesc:
		orderBy = "COALESCE(trx.balance, 0) DESC, w.id DESC"
	case DashboardSortTRXAsc:
		orderBy = "COALESCE(trx.balance, 0) ASC, w.id DESC"
	default:
		sort = DashboardSortUSDTDesc
	}

	listArgs := make([]any, 0, len(args)+2)
	listArgs = append(listArgs, args...)
	listArgs = append(listArgs, limit, offset)

	rows, err := d.sql.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			w.address_base58,
			COALESCE(trx.balance, 0) AS trx_balance,
			COALESCE(usdt.balance, 0) AS usdt_balance,
			CASE
				WHEN trx.updated_at IS NULL AND usdt.updated_at IS NULL THEN NULL
				WHEN trx.updated_at IS NULL THEN usdt.updated_at
				WHEN usdt.updated_at IS NULL THEN trx.updated_at
				WHEN trx.updated_at >= usdt.updated_at THEN trx.updated_at
				ELSE usdt.updated_at
			END AS last_updated_at
		FROM watch_addresses w
		LEFT JOIN asset_balances trx
			ON trx.address_base58 = w.address_base58
			AND trx.asset_code = 'TRX'
		LEFT JOIN asset_balances usdt
			ON usdt.address_base58 = w.address_base58
			AND usdt.asset_code = 'USDT'
		%s
		ORDER BY %s
		LIMIT ? OFFSET ?
	`, where, orderBy), listArgs...)
	if err != nil {
		return nil, fmt.Errorf("list dashboard rows: %w", err)
	}
	defer rows.Close()

	result := make([]DashboardRow, 0)
	for rows.Next() {
		var (
			item        DashboardRow
			trxBalance  string
			usdtBalance string
			lastUpdated sql.NullTime
		)
		if err := rows.Scan(&item.AddressBase58, &trxBalance, &usdtBalance, &lastUpdated); err != nil {
			return nil, fmt.Errorf("scan dashboard row: %w", err)
		}
		item.LastUpdatedAt = normalizeNullTime(lastUpdated)

		item.TRXBalance, err = decimal.NewFromString(trxBalance)
		if err != nil {
			return nil, fmt.Errorf("parse trx balance: %w", err)
		}
		item.USDTBalance, err = decimal.NewFromString(usdtBalance)
		if err != nil {
			return nil, fmt.Errorf("parse usdt balance: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &DashboardListResult{
		Rows:       result,
		TotalCount: totalCount,
	}, nil
}

func normalizeNullTime(input sql.NullTime) sql.NullTime {
	if !input.Valid {
		return input
	}
	return sql.NullTime{
		Time:  time.Date(input.Time.Year(), input.Time.Month(), input.Time.Day(), input.Time.Hour(), input.Time.Minute(), input.Time.Second(), 0, input.Time.Location()),
		Valid: true,
	}
}

func (d *DB) ListTransferInRecords(ctx context.Context, watchAddress string, limit, offset int, assetCode string, startTimeMs, endTimeMs int64) (*TransferListResult, error) {
	return d.listTransferRecords(ctx, "transfer_in_records", watchAddress, limit, offset, assetCode, startTimeMs, endTimeMs)
}

func (d *DB) ListTransferOutRecords(ctx context.Context, watchAddress string, limit, offset int, assetCode string, startTimeMs, endTimeMs int64) (*TransferListResult, error) {
	return d.listTransferRecords(ctx, "transfer_out_records", watchAddress, limit, offset, assetCode, startTimeMs, endTimeMs)
}

func (d *DB) ListBSCTransferInRecords(ctx context.Context, watchAddress string, limit, offset int, assetCode string, startTimeMs, endTimeMs int64) (*TransferListResult, error) {
	return d.listTransferRecords(ctx, "bsc_transfer_in_records", watchAddress, limit, offset, assetCode, startTimeMs, endTimeMs)
}

func (d *DB) ListBSCTransferOutRecords(ctx context.Context, watchAddress string, limit, offset int, assetCode string, startTimeMs, endTimeMs int64) (*TransferListResult, error) {
	return d.listTransferRecords(ctx, "bsc_transfer_out_records", watchAddress, limit, offset, assetCode, startTimeMs, endTimeMs)
}

func (d *DB) listTransferRecords(ctx context.Context, table, watchAddress string, limit, offset int, assetCode string, startTimeMs, endTimeMs int64) (*TransferListResult, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}

	where := "WHERE watch_address = ?"
	args := []any{watchAddress}
	if assetCode != "" {
		where += " AND asset_code = ?"
		args = append(args, assetCode)
	}
	if startTimeMs > 0 {
		where += " AND block_time >= ?"
		args = append(args, startTimeMs)
	}
	if endTimeMs > 0 {
		where += " AND block_time <= ?"
		args = append(args, endTimeMs)
	}

	var totalCount int
	countQuery := fmt.Sprintf("SELECT COUNT(1) FROM %s %s", table, where)
	if err := d.sql.QueryRowContext(ctx, countQuery, args...).Scan(&totalCount); err != nil {
		return nil, fmt.Errorf("count %s: %w", table, err)
	}

	listQuery := fmt.Sprintf(`
		SELECT
			tx_hash, block_number, block_time, asset_code, contract_address,
			watch_address, from_address, to_address, amount, log_index, status, created_at
		FROM %s
		%s
		ORDER BY block_number DESC, log_index DESC, id DESC
		LIMIT ? OFFSET ?
	`, table, where)

	listArgs := make([]any, 0, len(args)+2)
	listArgs = append(listArgs, args...)
	listArgs = append(listArgs, limit, offset)

	rows, err := d.sql.QueryContext(ctx, listQuery, listArgs...)
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", table, err)
	}
	defer rows.Close()

	records := make([]TransferListRecord, 0, limit)
	for rows.Next() {
		var (
			item       TransferListRecord
			amountText string
		)
		if err := rows.Scan(
			&item.TxHash,
			&item.BlockNumber,
			&item.BlockTime,
			&item.AssetCode,
			&item.ContractAddress,
			&item.WatchAddress,
			&item.FromAddress,
			&item.ToAddress,
			&amountText,
			&item.LogIndex,
			&item.Status,
			&item.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan %s: %w", table, err)
		}
		value, err := decimal.NewFromString(amountText)
		if err != nil {
			return nil, fmt.Errorf("parse amount: %w", err)
		}
		item.Amount = value
		records = append(records, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &TransferListResult{
		Records:    records,
		TotalCount: totalCount,
	}, nil
}
