const { pool } = require('../pool');

async function getPurchaseByTxHash(txHash, client = pool) {
  const result = await client.query(
    `
      SELECT *
      FROM purchases
      WHERE tx_hash = $1
      LIMIT 1
    `,
    [String(txHash).toLowerCase()]
  );

  return result.rows[0] || null;
}

async function upsertCandidatePurchase(payload, client = pool) {
  const normalizedTxHash = String(payload.txHash).toLowerCase();

  await client.query(
    `
      INSERT INTO purchases (
        tx_hash,
        purchase_id,
        buyer_wallet,
        candidate_slug_hash,
        candidate_ambassador_wallet,
        has_candidate_referral,
        status,
        created_at,
        updated_at
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,'detected',NOW(),NOW()
      )
      ON CONFLICT (tx_hash)
      DO UPDATE SET
        purchase_id = COALESCE(EXCLUDED.purchase_id, purchases.purchase_id),
        buyer_wallet = COALESCE(EXCLUDED.buyer_wallet, purchases.buyer_wallet),
        candidate_slug_hash = COALESCE(EXCLUDED.candidate_slug_hash, purchases.candidate_slug_hash),
        candidate_ambassador_wallet = COALESCE(EXCLUDED.candidate_ambassador_wallet, purchases.candidate_ambassador_wallet),
        has_candidate_referral = EXCLUDED.has_candidate_referral,
        updated_at = NOW()
    `,
    [
      normalizedTxHash,
      payload.purchaseId,
      payload.buyerWallet,
      payload.candidateSlugHash || null,
      payload.candidateAmbassadorWallet || null,
      Boolean(payload.candidateSlugHash || payload.candidateAmbassadorWallet)
    ]
  );
}

async function upsertPurchaseFromTokenEvent(payload, client = pool) {
  const normalizedTxHash = String(payload.txHash).toLowerCase();

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
      normalizedTxHash,
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

async function upsertReconciledPurchase(payload, client = pool) {
  const normalizedTxHash = String(payload.txHash).toLowerCase();
  const controllerProcessed = Boolean(payload.controllerProcessed);
  const hasCandidateReferral = Boolean(payload.candidateSlugHash || payload.candidateAmbassadorWallet);

  const status = payload.processingError
    ? 'error'
    : controllerProcessed
      ? 'processed'
      : payload.resolvedAmbassadorWallet
        ? 'attributed'
        : 'unattributed';

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
        status,
        binding_at_used,
        created_at,
        updated_at
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,
        $9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
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
        candidate_slug_hash = EXCLUDED.candidate_slug_hash,
        candidate_ambassador_wallet = EXCLUDED.candidate_ambassador_wallet,
        resolved_ambassador_wallet = EXCLUDED.resolved_ambassador_wallet,
        has_candidate_referral = EXCLUDED.has_candidate_referral,
        controller_processed = EXCLUDED.controller_processed,
        controller_processed_tx_hash = EXCLUDED.controller_processed_tx_hash,
        controller_processed_at = EXCLUDED.controller_processed_at,
        processing_error = EXCLUDED.processing_error,
        status = EXCLUDED.status,
        binding_at_used = EXCLUDED.binding_at_used,
        updated_at = NOW()
    `,
    [
      normalizedTxHash,
      payload.purchaseId,
      payload.buyerWallet,
      payload.purchaseAmountSun,
      payload.ownerShareSun,
      payload.tokenAmountRaw || null,
      payload.tokenBlockNumber || null,
      payload.tokenBlockTime || null,
      payload.candidateSlugHash || null,
      payload.candidateAmbassadorWallet || null,
      payload.resolvedAmbassadorWallet || null,
      hasCandidateReferral,
      controllerProcessed,
      payload.controllerProcessedTxHash || null,
      payload.controllerProcessedAt || null,
      payload.processingError || null,
      status,
      payload.bindingAtUsed || null
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
  const normalizedPurchaseId = String(purchaseId || '').toLowerCase();
  const normalizedControllerTxHash = String(txHash || '').toLowerCase();

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
         OR tx_hash = $1
         OR (buyer_wallet = $2 AND controller_processed = FALSE AND token_block_time <= $5)
    `,
    [
      normalizedPurchaseId,
      buyerWallet,
      ambassadorWallet,
      normalizedControllerTxHash,
      allocatedAt
    ]
  );
}

async function markPurchaseError({
  txHash,
  errorMessage
}, client = pool) {
  const normalizedTxHash = String(txHash || '').toLowerCase();

  await client.query(
    `
      INSERT INTO purchases (
        tx_hash,
        purchase_id,
        buyer_wallet,
        processing_error,
        status,
        created_at,
        updated_at
      )
      VALUES ($1,$1,'unknown',$2,'error',NOW(),NOW())
      ON CONFLICT (tx_hash)
      DO UPDATE SET
        processing_error = EXCLUDED.processing_error,
        status = 'error',
        updated_at = NOW()
    `,
    [normalizedTxHash, String(errorMessage || 'Unknown error')]
  );
}

async function recomputePurchaseStatuses(client = pool) {
  await client.query(
    `
      UPDATE purchases
      SET
        status = CASE
          WHEN processing_error IS NOT NULL THEN 'error'
          WHEN controller_processed THEN 'processed'
          ELSE 'unattributed'
        END,
        binding_at_used = NULL,
        resolved_ambassador_wallet = CASE
          WHEN controller_processed THEN resolved_ambassador_wallet
          ELSE NULL
        END,
        updated_at = NOW()
    `
  );

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
        WHERE p.controller_processed = FALSE
      )
      UPDATE purchases p
      SET
        binding_at_used = cb.binding_at,
        resolved_ambassador_wallet = cb.ambassador_wallet,
        status = 'attributed',
        updated_at = NOW()
      FROM chosen_bindings cb
      WHERE cb.purchase_row_id = p.id
        AND cb.rn = 1
    `
  );
}

module.exports = {
  getPurchaseByTxHash,
  upsertCandidatePurchase,
  upsertPurchaseFromTokenEvent,
  upsertReconciledPurchase,
  markPurchaseProcessed,
  markPurchaseError,
  recomputePurchaseStatuses
};
