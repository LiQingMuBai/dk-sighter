CREATE TABLE IF NOT EXISTS tron_sync_gaps (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  source_sync_key VARCHAR(64) NOT NULL DEFAULT '',
  from_block BIGINT NOT NULL,
  to_block BIGINT NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'pending',
  attempts INT NOT NULL DEFAULT 0,
  last_error TEXT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_tron_sync_gaps_status_from (status, from_block),
  KEY idx_tron_sync_gaps_created (created_at),
  KEY idx_tron_sync_gaps_source_status (source_sync_key, status)
);
