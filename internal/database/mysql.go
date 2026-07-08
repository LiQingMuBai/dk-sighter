package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"

	"tron_watcher/internal/config"
)

func NewMySQL(ctx context.Context, cfg config.MySQLConfig) (*sql.DB, error) {
	dsn, err := normalizeDSN(cfg)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(time.Duration(cfg.ConnMaxLifetime) * time.Second)

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	if err := setSessionTimeZone(ctx, db, cfg.SessionTimeZone); err != nil {
		return nil, err
	}

	return db, nil
}

func normalizeDSN(cfg config.MySQLConfig) (string, error) {
	parsed, err := mysqlDriver.ParseDSN(cfg.DSN)
	if err != nil {
		return "", fmt.Errorf("parse mysql dsn: %w", err)
	}

	if parsed.Params == nil {
		parsed.Params = map[string]string{}
	}

	if cfg.SessionTimeZone != "" {
		parsed.Params["time_zone"] = "'" + cfg.SessionTimeZone + "'"
	}

	return parsed.FormatDSN(), nil
}

func setSessionTimeZone(ctx context.Context, db *sql.DB, timeZone string) error {
	if timeZone == "" {
		return nil
	}
	if _, err := db.ExecContext(ctx, "SET time_zone = ?", timeZone); err != nil {
		return fmt.Errorf("set mysql session time_zone: %w", err)
	}
	return nil
}
