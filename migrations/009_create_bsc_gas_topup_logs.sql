CREATE TABLE IF NOT EXISTS bsc_gas_topup_logs (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  address VARCHAR(128) NOT NULL,
  from_address VARCHAR(128) NOT NULL,
  amount_bnb DECIMAL(36,18) NOT NULL DEFAULT 0,
  current_bnb DECIMAL(36,18) NULL,
  current_usdt DECIMAL(36,18) NULL,
  tx_hash VARCHAR(128) NULL,
  key_source VARCHAR(255) NOT NULL DEFAULT '',
  status VARCHAR(16) NOT NULL,
  response_body TEXT NULL,
  error_message TEXT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_bsc_gas_topup_created_status (created_at, status),
  KEY idx_bsc_gas_topup_address_created (address, created_at)
);
