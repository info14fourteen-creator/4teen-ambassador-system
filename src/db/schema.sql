CREATE TABLE IF NOT EXISTS purchases (
  id BIGSERIAL PRIMARY KEY,
  tx_hash TEXT NOT NULL UNIQUE,
  purchase_id TEXT NOT NULL UNIQUE,
  buyer_wallet TEXT NOT NULL,
  purchase_amount_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  owner_share_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
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
  processing_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_purchases_buyer_wallet
  ON purchases (buyer_wallet);

CREATE INDEX IF NOT EXISTS idx_purchases_resolved_ambassador_wallet
  ON purchases (resolved_ambassador_wallet);

CREATE INDEX IF NOT EXISTS idx_purchases_token_block_number
  ON purchases (token_block_number);

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

CREATE TABLE IF NOT EXISTS sync_state (
  state_key TEXT PRIMARY KEY,
  state_value TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
