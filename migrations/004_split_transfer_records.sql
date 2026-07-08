CREATE TABLE IF NOT EXISTS transfer_in_records (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  tx_hash VARCHAR(128) NOT NULL,
  block_number BIGINT NOT NULL,
  block_time BIGINT NOT NULL,
  asset_code VARCHAR(16) NOT NULL,
  contract_address VARCHAR(64) DEFAULT NULL,
  from_address VARCHAR(64) NOT NULL,
  to_address VARCHAR(64) NOT NULL,
  amount DECIMAL(36,6) NOT NULL,
  log_index INT NOT NULL DEFAULT 0,
  status VARCHAR(16) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_tx_asset_log (tx_hash, asset_code, log_index),
  KEY idx_from_asset_block (from_address, asset_code, block_number),
  KEY idx_to_asset_block (to_address, asset_code, block_number)
);

CREATE TABLE IF NOT EXISTS transfer_out_records (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  tx_hash VARCHAR(128) NOT NULL,
  block_number BIGINT NOT NULL,
  block_time BIGINT NOT NULL,
  asset_code VARCHAR(16) NOT NULL,
  contract_address VARCHAR(64) DEFAULT NULL,
  from_address VARCHAR(64) NOT NULL,
  to_address VARCHAR(64) NOT NULL,
  amount DECIMAL(36,6) NOT NULL,
  log_index INT NOT NULL DEFAULT 0,
  status VARCHAR(16) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_tx_asset_log (tx_hash, asset_code, log_index),
  KEY idx_from_asset_block (from_address, asset_code, block_number),
  KEY idx_to_asset_block (to_address, asset_code, block_number)
);

CREATE TABLE IF NOT EXISTS bsc_transfer_in_records (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  tx_hash VARCHAR(128) NOT NULL,
  block_number BIGINT NOT NULL,
  block_time BIGINT NOT NULL,
  asset_code VARCHAR(16) NOT NULL,
  contract_address VARCHAR(64) DEFAULT NULL,
  from_address VARCHAR(64) NOT NULL,
  to_address VARCHAR(64) NOT NULL,
  amount DECIMAL(36,18) NOT NULL,
  log_index INT NOT NULL DEFAULT 0,
  status VARCHAR(16) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_tx_asset_log (tx_hash, asset_code, log_index),
  KEY idx_from_asset_block (from_address, asset_code, block_number),
  KEY idx_to_asset_block (to_address, asset_code, block_number)
);

CREATE TABLE IF NOT EXISTS bsc_transfer_out_records (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  tx_hash VARCHAR(128) NOT NULL,
  block_number BIGINT NOT NULL,
  block_time BIGINT NOT NULL,
  asset_code VARCHAR(16) NOT NULL,
  contract_address VARCHAR(64) DEFAULT NULL,
  from_address VARCHAR(64) NOT NULL,
  to_address VARCHAR(64) NOT NULL,
  amount DECIMAL(36,18) NOT NULL,
  log_index INT NOT NULL DEFAULT 0,
  status VARCHAR(16) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_tx_asset_log (tx_hash, asset_code, log_index),
  KEY idx_from_asset_block (from_address, asset_code, block_number),
  KEY idx_to_asset_block (to_address, asset_code, block_number)
);
