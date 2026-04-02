ALTER TABLE ambassadors
  ADD COLUMN IF NOT EXISTS slug TEXT,
  ADD COLUMN IF NOT EXISTS slug_hash TEXT,
  ADD COLUMN IF NOT EXISTS meta_hash TEXT,
  ADD COLUMN IF NOT EXISTS exists_on_chain BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS self_registered BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS manual_assigned BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS override_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS current_level INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS override_level INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS effective_level INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS reward_percent NUMERIC(78,0) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS created_at_chain BIGINT,
  ADD COLUMN IF NOT EXISTS total_buyers NUMERIC(78,0) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS total_volume_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS total_rewards_accrued_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS total_rewards_claimed_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS claimable_rewards_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_chain_sync_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_ambassadors_slug_hash
  ON ambassadors (slug_hash);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ambassadors_slug_unique
  ON ambassadors (slug)
  WHERE slug IS NOT NULL;

ALTER TABLE purchases
  ADD COLUMN IF NOT EXISTS ambassador_reward_sun NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS reward_percent NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS controller_reward_sun NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS controller_owner_part_sun NUMERIC(78,0),
  ADD COLUMN IF NOT EXISTS controller_level INTEGER,
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'detected',
  ADD COLUMN IF NOT EXISTS binding_at_used TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_purchases_status
  ON purchases (status);

CREATE TABLE IF NOT EXISTS buyer_bindings (
  id BIGSERIAL PRIMARY KEY,
  buyer_wallet TEXT NOT NULL,
  ambassador_wallet TEXT NOT NULL,
  old_ambassador_wallet TEXT,
  new_ambassador_wallet TEXT,
  binding_at TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL,
  event_name TEXT NOT NULL,
  binding_tx_hash TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_buyer_bindings_buyer_wallet
  ON buyer_bindings (buyer_wallet);

CREATE INDEX IF NOT EXISTS idx_buyer_bindings_ambassador_wallet
  ON buyer_bindings (ambassador_wallet);

CREATE INDEX IF NOT EXISTS idx_buyer_bindings_binding_at
  ON buyer_bindings (binding_at);

CREATE TABLE IF NOT EXISTS controller_purchase_allocations (
  purchase_id TEXT PRIMARY KEY,
  tx_hash TEXT NOT NULL UNIQUE,
  buyer_wallet TEXT NOT NULL,
  ambassador_wallet TEXT NOT NULL,
  purchase_amount_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  owner_share_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  reward_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  owner_part_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  level INTEGER NOT NULL DEFAULT 0,
  allocated_at TIMESTAMPTZ,
  allocation_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE controller_purchase_allocations
  ALTER COLUMN allocated_at DROP NOT NULL;

ALTER TABLE controller_purchase_allocations
  ADD COLUMN IF NOT EXISTS allocation_at TIMESTAMPTZ;

UPDATE controller_purchase_allocations
SET allocation_at = allocated_at
WHERE allocation_at IS NULL
  AND allocated_at IS NOT NULL;

UPDATE controller_purchase_allocations
SET allocated_at = allocation_at
WHERE allocated_at IS NULL
  AND allocation_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_controller_purchase_allocations_buyer_wallet
  ON controller_purchase_allocations (buyer_wallet);

CREATE INDEX IF NOT EXISTS idx_controller_purchase_allocations_ambassador_wallet
  ON controller_purchase_allocations (ambassador_wallet);

CREATE INDEX IF NOT EXISTS idx_controller_purchase_allocations_allocated_at
  ON controller_purchase_allocations (allocated_at);

CREATE TABLE IF NOT EXISTS ambassador_reward_withdrawals (
  id BIGSERIAL PRIMARY KEY,
  ambassador_wallet TEXT NOT NULL,
  amount_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  tx_hash TEXT NOT NULL UNIQUE,
  block_time TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ambassador_reward_withdrawals_ambassador_wallet
  ON ambassador_reward_withdrawals (ambassador_wallet);

CREATE INDEX IF NOT EXISTS idx_ambassador_reward_withdrawals_block_time
  ON ambassador_reward_withdrawals (block_time);

CREATE TABLE IF NOT EXISTS ambassador_buyers (
  buyer_wallet TEXT NOT NULL,
  ambassador_wallet TEXT NOT NULL,
  binding_at TIMESTAMPTZ,
  first_attributed_purchase_at TIMESTAMPTZ,
  last_attributed_purchase_at TIMESTAMPTZ,
  purchase_count INTEGER NOT NULL DEFAULT 0,
  total_purchase_amount_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  total_owner_share_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  total_reward_amount_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  processed_purchase_count INTEGER NOT NULL DEFAULT 0,
  processed_purchase_amount_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  processed_reward_amount_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  pending_purchase_count INTEGER NOT NULL DEFAULT 0,
  pending_purchase_amount_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  pending_reward_amount_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (buyer_wallet, ambassador_wallet)
);

CREATE INDEX IF NOT EXISTS idx_ambassador_buyers_ambassador_wallet
  ON ambassador_buyers (ambassador_wallet);
