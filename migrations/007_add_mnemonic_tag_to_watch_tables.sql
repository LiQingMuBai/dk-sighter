ALTER TABLE watch_addresses
  ADD COLUMN mnemonic_tag VARCHAR(64) NOT NULL DEFAULT '' AFTER wallet_index,
  ADD KEY idx_watch_addresses_source_mnemonic_index (source, mnemonic_tag, wallet_index);

ALTER TABLE bsc_watch_addresses
  ADD COLUMN mnemonic_tag VARCHAR(64) NOT NULL DEFAULT '' AFTER wallet_index,
  ADD KEY idx_bsc_watch_addresses_source_mnemonic_index (source, mnemonic_tag, wallet_index);
