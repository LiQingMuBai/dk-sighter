ALTER TABLE transfer_in_records
  ADD COLUMN watch_address VARCHAR(128) NOT NULL DEFAULT '' AFTER contract_address,
  ADD KEY idx_watch_asset_block (watch_address, asset_code, block_number);

ALTER TABLE transfer_out_records
  ADD COLUMN watch_address VARCHAR(128) NOT NULL DEFAULT '' AFTER contract_address,
  ADD KEY idx_watch_asset_block (watch_address, asset_code, block_number);

ALTER TABLE bsc_transfer_in_records
  ADD COLUMN watch_address VARCHAR(128) NOT NULL DEFAULT '' AFTER contract_address,
  ADD KEY idx_watch_asset_block (watch_address, asset_code, block_number);

ALTER TABLE bsc_transfer_out_records
  ADD COLUMN watch_address VARCHAR(128) NOT NULL DEFAULT '' AFTER contract_address,
  ADD KEY idx_watch_asset_block (watch_address, asset_code, block_number);
