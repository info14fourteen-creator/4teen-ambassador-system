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

async function upsertCandidatePurchase({
  txHash,
  purchaseId,
  buyerWallet,
  candidateSlugHash,
  candidateAmbassadorWallet
}, client = pool) {
  await client.query(
    `
      INSERT INTO purchases (
        tx_hash,
        purchase_id,
        buyer_wallet,
        candidate_slug_hash,
        candidate_ambassador_wallet,
        has_candidate_referral,
        created_at,
        updated_at
      )
      VALUES ($1,$2,$3,$4,$5,$6,NOW(),NOW())
      ON CONFLICT (tx_hash)
      DO UPDATE SET
        candidate_slug_hash = COALESCE(EXCLUDED.candidate_slug_hash, purchases.candidate_slug_hash),
        candidate_ambassador_wallet = COALESCE(EXCLUDED.candidate_ambassador_wallet, purchases.candidate_ambassador_wallet),
        has_candidate_referral = EXCLUDED.has_candidate_referral,
        updated_at = NOW()
    `,
    [
      txHash,
      purchaseId,
      buyerWallet,
      candidateSlugHash || null,
      candidateAmbassadorWallet || null,
      Boolean(candidateSlugHash || candidateAmbassadorWallet)
    ]
  );
}

async function upsertReconciledPurchase(payload, client = pool) {
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
        candidate_slug_hash,
        candidate_ambassador_wallet,
        resolved_ambassador_wallet,
        has_candidate_referral,
        controller_processed,
        controller_processed_tx_hash,
        controller_processed_at,
        processing_error,
        updated_at
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
        CASE WHEN $13 THEN NOW() ELSE NULL END,
        NULL,
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
        candidate_slug_hash = COALESCE(EXCLUDED.candidate_slug_hash, purchases.candidate_slug_hash),
        candidate_ambassador_wallet = COALESCE(EXCLUDED.candidate_ambassador_wallet, purchases.candidate_ambassador_wallet),
        resolved_ambassador_wallet = EXCLUDED.resolved_ambassador_wallet,
        has_candidate_referral = EXCLUDED.has_candidate_referral,
        controller_processed = EXCLUDED.controller_processed,
        controller_processed_tx_hash = COALESCE(EXCLUDED.controller_processed_tx_hash, purchases.controller_processed_tx_hash),
        controller_processed_at = CASE
          WHEN EXCLUDED.controller_processed THEN COALESCE(purchases.controller_processed_at, NOW())
          ELSE purchases.controller_processed_at
        END,
        processing_error = NULL,
        updated_at = NOW()
    `,
    [
      payload.txHash,
      payload.purchaseId,
      payload.buyerWallet,
      payload.purchaseAmountSun,
      payload.ownerShareSun,
      payload.tokenAmountRaw || null,
      payload.tokenBlockNumber,
      payload.tokenBlockTime,
      payload.candidateSlugHash || null,
      payload.candidateAmbassadorWallet || null,
      payload.resolvedAmbassadorWallet || null,
      Boolean(payload.candidateSlugHash || payload.candidateAmbassadorWallet),
      Boolean(payload.controllerProcessed),
      payload.controllerProcessedTxHash || null
    ]
  );
}

async function markPurchaseError({
  txHash,
  errorMessage
}, client = pool) {
  await client.query(
    `
      INSERT INTO purchases (
        tx_hash,
        purchase_id,
        buyer_wallet,
        processing_error,
        updated_at
      )
      VALUES ($1,$2,$3,$4,NOW())
      ON CONFLICT (tx_hash)
      DO UPDATE SET
        processing_error = EXCLUDED.processing_error,
        updated_at = NOW()
    `,
    [txHash, `failed:${txHash}`, 'unknown', errorMessage]
  );
}

module.exports = {
  getPurchaseByTxHash,
  upsertCandidatePurchase,
  upsertReconciledPurchase,
  markPurchaseError
};
