CREATE TABLE IF NOT EXISTS tron_activation_logs (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  job_id VARCHAR(32) NULL,
  address_base58 VARCHAR(64) NOT NULL,
  from_address_base58 VARCHAR(64) NOT NULL,
  amount_sun BIGINT NOT NULL DEFAULT 0,
  txid VARCHAR(128) NULL,
  status VARCHAR(16) NOT NULL,
  error_message TEXT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_address_created (address_base58, created_at),
  KEY idx_job_created (job_id, created_at),
  KEY idx_status_created (status, created_at)
);

