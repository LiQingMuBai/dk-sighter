-- Performance indexes for HD-wallet Dashboard (Tron + BSC) on MySQL 8.0.
-- Goal: eliminate full-scan JOIN amplification on bnb/usdt lookups and speed up
-- the WHERE (status, source, mnemonic_tag) filter + GROUP BY LOWER(address).
-- These statements are idempotent on fresh DBs; on already-migrated instances
-- re-running them will error with "Duplicate key name" / "Check that column/key
-- exists" which is safe to ignore.

DROP INDEX idx_bsc_asset_code_updated_at ON bsc_asset_balances;

ALTER TABLE bsc_asset_balances
  ADD INDEX idx_bab_lower_addr_asset ((LOWER(address)), asset_code, updated_at);

ALTER TABLE asset_balances
  ADD INDEX idx_ab_addr_asset_updated (address_base58, asset_code, updated_at);

ALTER TABLE watch_addresses
  ADD INDEX idx_wa_ssm (status, source, mnemonic_tag, wallet_index, id);

ALTER TABLE bsc_watch_addresses
  ADD INDEX idx_bwa_ssm (status, source, mnemonic_tag, wallet_index, id);

ALTER TABLE bsc_watch_addresses
  ADD INDEX idx_bwa_lower_addr ((LOWER(address)));
