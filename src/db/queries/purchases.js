const { pool } = require('../pool');

async function getPurchaseByTxHash(txHash, client = pool) {
  const result = await client.query(
    `
      SELECT *
      FROM purchases
      WHERE tx_hash = $1
      LIMIT 1
    `,
    [txHash]
  );

  return result.rows[0] || null;
}

async function upsertPurchaseFromTokenEvent(payload, client = pool) {
  await client.query(
    `
      INSERT INTO purchases (
        tx_hash,
        purchase_id,
        buyer_wallet,
        purchase_amount_sun,
        owner_share_sun,
        token_amount_raw,
        token_block_number,
        token_block_time,
        resolved_ambassador_wallet,
        controller_processed,
        controller_processed_tx_hash,
        controller_processed_at,
        processing_error,
        status,
        binding_at_used,
        created_at,
        updated_at
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,
        NULL,
        FALSE,
        NULL,
        NULL,
        NULL,
        'detected',
        NULL,
        NOW(),
        NOW()
      )
      ON CONFLICT (tx_hash)
      DO UPDATE SET
        purchase_id = EXCLUDED.purchase_id,
        buyer_wallet = EXCLUDED.buyer_wallet,
        purchase_amount_sun = EXCLUDED.purchase_amount_sun,
        owner_share_sun = EXCLUDED.owner_share_sun,
        token_amount_raw = EXCLUDED.token_amount_raw,
        token_block_number = EXCLUDED.token_block_number,
        token_block_time = EXCLUDED.token_block_time,
        updated_at = NOW()
    `,
    [
      payload.txHash,
      payload.purchaseId,
      payload.buyerWallet,
      payload.purchaseAmountSun,
      payload.ownerShareSun,
      payload.tokenAmountRaw,
      payload.tokenBlockNumber,
      payload.tokenBlockTime
    ]
  );
}

async function markPurchaseProcessed({
  purchaseId,
  buyerWallet,
  ambassadorWallet,
  txHash,
  allocatedAt
}, client = pool) {
  await client.query(
    `
      UPDATE purchases
      SET
        resolved_ambassador_wallet = $3,
        controller_processed = TRUE,
        controller_processed_tx_hash = $4,
        controller_processed_at = $5,
        status = 'processed',
        updated_at = NOW()
      WHERE purchase_id = $1
         OR (buyer_wallet = $2 AND tx_hash = $4)
    `,
    [purchaseId, buyerWallet, ambassadorWallet, txHash, allocatedAt]
  );
}

async function recomputePurchaseStatuses(client = pool) {
  await client.query(
    `
      WITH chosen_bindings AS (
        SELECT
          p.id AS purchase_row_id,
          bb.ambassador_wallet,
          bb.binding_at,
          ROW_NUMBER() OVER (
            PARTITION BY p.id
            ORDER BY bb.binding_at DESC, bb.id DESC
          ) AS rn
        FROM purchases p
        JOIN buyer_bindings bb
          ON bb.buyer_wallet = p.buyer_wallet
         AND bb.binding_at <= COALESCE(p.token_block_time, NOW())
      )
      UPDATE purchases p
      SET
        binding_at_used = cb.binding_at,
        resolved_ambassador_wallet = COALESCE(p.resolved_ambassador_wallet, cb.ambassador_wallet),
        status = CASE
          WHEN p.processing_error IS NOT NULL THEN 'error'
          WHEN p.controller_processed THEN 'processed'
          WHEN COALESCE(p.resolved_ambassador_wallet, cb.ambassador_wallet) IS NOT NULL THEN 'attributed'
          ELSE 'unattributed'
        END,
        updated_at = NOW()
      FROM chosen_bindings cb
      WHERE cb.purchase_row_id = p.id
        AND cb.rn = 1
    `
  );

  await client.query(
    `
      UPDATE purchases
      SET
        status = CASE
          WHEN processing_error IS NOT NULL THEN 'error'
          WHEN controller_processed THEN 'processed'
          WHEN resolved_ambassador_wallet IS NOT NULL THEN 'attributed'
          ELSE 'unattributed'
        END,
        updated_at = NOW()
    `
  );
}

module.exports = {
  getPurchaseByTxHash,
  upsertPurchaseFromTokenEvent,
  markPurchaseProcessed,
  recomputePurchaseStatuses
};
