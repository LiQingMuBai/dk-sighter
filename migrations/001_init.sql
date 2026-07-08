CREATE TABLE IF NOT EXISTS watch_addresses (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  address_base58 VARCHAR(64) NOT NULL UNIQUE,
  status TINYINT NOT NULL DEFAULT 1,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS asset_balances (
  address_base58 VARCHAR(64) NOT NULL,
  asset_code VARCHAR(16) NOT NULL,
  balance DECIMAL(36,6) NOT NULL DEFAULT 0,
  block_number BIGINT NOT NULL DEFAULT 0,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (address_base58, asset_code)
);

CREATE TABLE IF NOT EXISTS transfer_records (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  tx_hash VARCHAR(128) NOT NULL,
  block_number BIGINT NOT NULL,
  block_time BIGINT NOT NULL,
  asset_code VARCHAR(16) NOT NULL,
  contract_address VARCHAR(64) DEFAULT NULL,
  from_address VARCHAR(64) NOT NULL,
  to_address VARCHAR(64) NOT NULL,
  amount DECIMAL(36,6) NOT NULL,
  direction VARCHAR(16) NOT NULL,
  log_index INT NOT NULL DEFAULT 0,
  status VARCHAR(16) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_tx_asset_log (tx_hash, asset_code, log_index),
  KEY idx_from_asset_block (from_address, asset_code, block_number),
  KEY idx_to_asset_block (to_address, asset_code, block_number)
);

CREATE TABLE IF NOT EXISTS sync_state (
  sync_key VARCHAR(64) PRIMARY KEY,
  last_block BIGINT NOT NULL DEFAULT 0,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS runtime_settings (
  setting_key VARCHAR(64) PRIMARY KEY,
  setting_value VARCHAR(255) NOT NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO runtime_settings (setting_key, setting_value)
VALUES ('energy_provider', 'trxfee')
ON DUPLICATE KEY UPDATE setting_value = setting_value;

CREATE TABLE IF NOT EXISTS energy_action_logs (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  action_name VARCHAR(32) NOT NULL,
  address_base58 VARCHAR(64) NOT NULL,
  provider VARCHAR(32) NOT NULL,
  energy_amount INT NOT NULL DEFAULT 0,
  action_score INT NOT NULL DEFAULT 0,
  status VARCHAR(16) NOT NULL,
  response_body TEXT NULL,
  error_message TEXT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_created_status (created_at, status),
  KEY idx_address_created (address_base58, created_at)
);
