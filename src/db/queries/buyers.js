const { pool } = require('../pool');

async function upsertBuyer({
  buyerWallet,
  boundAmbassadorWallet,
  txHash,
  blockTime
}, client = pool) {
  await client.query(
    `
      INSERT INTO buyers (
        buyer_wallet,
        bound_ambassador_wallet,
        first_purchase_tx_hash,
        first_purchase_at,
        last_purchase_tx_hash,
        last_purchase_at,
        last_chain_sync_at,
        updated_at
      )
      VALUES ($1,$2,$3,$4,$3,$4,NOW(),NOW())
      ON CONFLICT (buyer_wallet)
      DO UPDATE SET
        bound_ambassador_wallet = EXCLUDED.bound_ambassador_wallet,
        last_purchase_tx_hash = EXCLUDED.last_purchase_tx_hash,
        last_purchase_at = EXCLUDED.last_purchase_at,
        last_chain_sync_at = NOW(),
        updated_at = NOW()
    `,
    [buyerWallet, boundAmbassadorWallet || null, txHash, blockTime]
  );
}

module.exports = {
  upsertBuyer
};
