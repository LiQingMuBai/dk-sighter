ALTER TABLE watch_addresses
  ADD COLUMN source VARCHAR(32) NOT NULL DEFAULT 'watcher' AFTER address_base58,
  ADD COLUMN wallet_index INT DEFAULT NULL AFTER source,
  ADD KEY idx_watch_addresses_source_status (source, status),
  ADD KEY idx_watch_addresses_source_index (source, wallet_index);

ALTER TABLE bsc_watch_addresses
  ADD COLUMN source VARCHAR(32) NOT NULL DEFAULT 'watcher' AFTER address,
  ADD COLUMN wallet_index INT DEFAULT NULL AFTER source,
  ADD KEY idx_bsc_watch_addresses_source_status (source, status),
  ADD KEY idx_bsc_watch_addresses_source_index (source, wallet_index);
