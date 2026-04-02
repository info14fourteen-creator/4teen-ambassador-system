const { pool } = require('../pool');

async function upsertBuyerBinding(payload, client = pool) {
  await client.query(
    `
      INSERT INTO buyer_bindings (
        buyer_wallet,
        ambassador_wallet,
        old_ambassador_wallet,
        binding_at,
        source,
        event_name,
        binding_tx_hash,
        created_at,
        updated_at
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,NOW(),NOW())
      ON CONFLICT (binding_tx_hash)
      DO UPDATE SET
        buyer_wallet = EXCLUDED.buyer_wallet,
        ambassador_wallet = EXCLUDED.ambassador_wallet,
        old_ambassador_wallet = EXCLUDED.old_ambassador_wallet,
        binding_at = EXCLUDED.binding_at,
        source = EXCLUDED.source,
        event_name = EXCLUDED.event_name,
        updated_at = NOW()
    `,
    [
      payload.buyerWallet,
      payload.ambassadorWallet,
      payload.oldAmbassadorWallet || null,
      payload.bindingAt,
      payload.source,
      payload.eventName,
      payload.bindingTxHash
    ]
  );
}

module.exports = {
  upsertBuyerBinding
};
