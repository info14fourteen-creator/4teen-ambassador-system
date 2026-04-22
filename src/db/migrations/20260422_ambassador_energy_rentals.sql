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
