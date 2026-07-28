ALTER TABLE sync_gaps
  ADD COLUMN repair_owner VARCHAR(128) NOT NULL DEFAULT '' AFTER status;

ALTER TABLE sync_gaps
  ADD KEY idx_sync_gaps_chain_status_owner_from (chain, status, repair_owner, from_block);
