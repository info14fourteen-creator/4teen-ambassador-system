CREATE TABLE IF NOT EXISTS purchases (
  id BIGSERIAL PRIMARY KEY,
  tx_hash TEXT NOT NULL UNIQUE,
  purchase_id TEXT NOT NULL UNIQUE,
  buyer_wallet TEXT NOT NULL,
  purchase_amount_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  owner_share_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  ambassador_reward_sun NUMERIC(78,0),
  reward_percent NUMERIC(78,0),
  token_amount_raw NUMERIC(78,0),
  token_block_number BIGINT,
  token_block_time TIMESTAMPTZ,
  candidate_slug_hash TEXT,
  candidate_ambassador_wallet TEXT,
  resolved_ambassador_wallet TEXT,
  has_candidate_referral BOOLEAN NOT NULL DEFAULT FALSE,
  controller_processed BOOLEAN NOT NULL DEFAULT FALSE,
  controller_processed_tx_hash TEXT,
  controller_processed_at TIMESTAMPTZ,
  controller_reward_sun NUMERIC(78,0),
  controller_owner_part_sun NUMERIC(78,0),
  controller_level INTEGER,
  processing_error TEXT,
  status TEXT NOT NULL DEFAULT 'detected',
  binding_at_used TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_purchases_buyer_wallet
  ON purchases (buyer_wallet);

CREATE INDEX IF NOT EXISTS idx_purchases_resolved_ambassador_wallet
  ON purchases (resolved_ambassador_wallet);

CREATE INDEX IF NOT EXISTS idx_purchases_token_block_number
  ON purchases (token_block_number);

CREATE INDEX IF NOT EXISTS idx_purchases_status
  ON purchases (status);

CREATE TABLE IF NOT EXISTS buyers (
  buyer_wallet TEXT PRIMARY KEY,
  bound_ambassador_wallet TEXT,
  first_purchase_tx_hash TEXT,
  first_purchase_at TIMESTAMPTZ,
  last_purchase_tx_hash TEXT,
  last_purchase_at TIMESTAMPTZ,
  last_chain_sync_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_buyers_bound_ambassador_wallet
  ON buyers (bound_ambassador_wallet);

CREATE TABLE IF NOT EXISTS ambassadors (
  ambassador_wallet TEXT PRIMARY KEY,
  slug TEXT,
  slug_hash TEXT,
  meta_hash TEXT,
  exists_on_chain BOOLEAN NOT NULL DEFAULT FALSE,
  active BOOLEAN NOT NULL DEFAULT FALSE,
  self_registered BOOLEAN NOT NULL DEFAULT FALSE,
  manual_assigned BOOLEAN NOT NULL DEFAULT FALSE,
  override_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  current_level INTEGER NOT NULL DEFAULT 0,
  override_level INTEGER NOT NULL DEFAULT 0,
  effective_level INTEGER NOT NULL DEFAULT 0,
  reward_percent NUMERIC(78,0) NOT NULL DEFAULT 0,
  created_at_chain BIGINT,
  total_buyers NUMERIC(78,0) NOT NULL DEFAULT 0,
  total_volume_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  total_rewards_accrued_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  total_rewards_claimed_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  claimable_rewards_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  last_chain_sync_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ambassadors_slug_hash
  ON ambassadors (slug_hash);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ambassadors_slug_unique
  ON ambassadors (slug)
  WHERE slug IS NOT NULL;

CREATE TABLE IF NOT EXISTS sync_state (
  state_key TEXT PRIMARY KEY,
  state_value TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
  allocated_at TIMESTAMPTZ NOT NULL,
  allocation_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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

CREATE TABLE IF NOT EXISTS ambassador_energy_rentals (
  id BIGSERIAL PRIMARY KEY,
  wallet TEXT NOT NULL,
  slug TEXT,
  payment_tx_hash TEXT UNIQUE NOT NULL,
  payment_amount_sun NUMERIC(78,0) NOT NULL,
  energy_quantity INTEGER NOT NULL,
  request_id TEXT,
  trade_no TEXT,
  status TEXT NOT NULL DEFAULT 'paid',
  row_json JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ambassador_energy_rentals_wallet
  ON ambassador_energy_rentals (wallet);

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
