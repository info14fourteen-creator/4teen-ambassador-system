ALTER TABLE purchases
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'detected',
  ADD COLUMN IF NOT EXISTS binding_at_used TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_purchases_status
  ON purchases (status);

CREATE TABLE IF NOT EXISTS buyer_bindings (
  id BIGSERIAL PRIMARY KEY,
  buyer_wallet TEXT NOT NULL,
  ambassador_wallet TEXT NOT NULL,
  old_ambassador_wallet TEXT,
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
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_controller_purchase_allocations_buyer_wallet
  ON controller_purchase_allocations (buyer_wallet);

CREATE INDEX IF NOT EXISTS idx_controller_purchase_allocations_ambassador_wallet
  ON controller_purchase_allocations (ambassador_wallet);

CREATE INDEX IF NOT EXISTS idx_controller_purchase_allocations_allocated_at
  ON controller_purchase_allocations (allocated_at);

CREATE TABLE IF NOT EXISTS ambassador_buyers (
  buyer_wallet TEXT NOT NULL,
  ambassador_wallet TEXT NOT NULL,
  binding_at TIMESTAMPTZ,
  first_attributed_purchase_at TIMESTAMPTZ,
  last_attributed_purchase_at TIMESTAMPTZ,
  purchase_count INTEGER NOT NULL DEFAULT 0,
  total_purchase_amount_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  total_owner_share_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  processed_purchase_count INTEGER NOT NULL DEFAULT 0,
  processed_purchase_amount_sun NUMERIC(78,0) NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (buyer_wallet, ambassador_wallet)
);

CREATE INDEX IF NOT EXISTS idx_ambassador_buyers_ambassador_wallet
  ON ambassador_buyers (ambassador_wallet);
