const { pool } = require('../pool');

function normalizeLower(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizeBytes32(value) {
  return normalizeLower(value).replace(/^0x/, '');
}

async function getPurchaseByTxHash(txHash, client = pool) {
  const result = await client.query(
    `
      SELECT *
      FROM purchases
      WHERE tx_hash = $1
      LIMIT 1
    `,
    [normalizeLower(txHash)]
  );

  return result.rows[0] || null;
}

async function upsertCandidatePurchase(payload, client = pool) {
  const normalizedTxHash = normalizeLower(payload.txHash);

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
  const normalizedTxHash = normalizeLower(payload.txHash);

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
  const normalizedTxHash = normalizeLower(payload.txHash);
  const controllerProcessed = Boolean(payload.controllerProcessed);
  const hasCandidateReferral = Boolean(payload.candidateSlugHash || payload.candidateAmbassadorWallet);

  const status = payload.status || (
    payload.processingError
      ? 'error'
      : controllerProcessed
        ? 'processed'
        : payload.resolvedAmbassadorWallet
          ? 'attributed'
          : 'unattributed'
  );

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
        candidate_slug_hash = COALESCE(EXCLUDED.candidate_slug_hash, purchases.candidate_slug_hash),
        candidate_ambassador_wallet = COALESCE(EXCLUDED.candidate_ambassador_wallet, purchases.candidate_ambassador_wallet),
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
  const normalizedPurchaseId = normalizeLower(purchaseId);
  const normalizedControllerTxHash = normalizeLower(txHash);

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
  const normalizedTxHash = normalizeLower(txHash);

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
          WHEN processing_error IS NOT NULL AND controller_processed = FALSE AND resolved_ambassador_wallet IS NULL THEN status
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

async function getReplayablePendingPurchases(ambassadorWallet, limit = 10, client = pool) {
  const normalizedLimit = Math.max(1, Math.min(Number(limit || 10), 50));

  const result = await client.query(
    `
      SELECT
        id,
        tx_hash,
        purchase_id,
        buyer_wallet,
        purchase_amount_sun,
        owner_share_sun,
        resolved_ambassador_wallet,
        token_block_time,
        status,
        controller_processed,
        controller_processed_tx_hash
      FROM purchases
      WHERE resolved_ambassador_wallet = $1
        AND status = 'attributed'
        AND controller_processed = FALSE
      ORDER BY token_block_time ASC NULLS LAST, id ASC
      LIMIT $2
    `,
    [ambassadorWallet, normalizedLimit]
  );

  return result.rows;
}

async function applyAllocationResult(payload, client = pool) {
  const normalizedPurchaseId = normalizeBytes32(payload.purchaseId);
  const normalizedAllocationTxHash = normalizeLower(payload.txHash);
  const normalizedPurchaseTxHash = normalizeLower(payload.purchaseTxHash);
  const allocationAt = payload.allocationAt || payload.blockTime || new Date().toISOString();

  await client.query(
    `
      INSERT INTO controller_purchase_allocations (
        purchase_id,
        tx_hash,
        buyer_wallet,
        ambassador_wallet,
        purchase_amount_sun,
        owner_share_sun,
        reward_sun,
        owner_part_sun,
        level,
        allocated_at,
        allocation_at,
        created_at,
        updated_at
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$10,NOW(),NOW()
      )
      ON CONFLICT (purchase_id)
      DO UPDATE SET
        tx_hash = EXCLUDED.tx_hash,
        buyer_wallet = EXCLUDED.buyer_wallet,
        ambassador_wallet = EXCLUDED.ambassador_wallet,
        purchase_amount_sun = EXCLUDED.purchase_amount_sun,
        owner_share_sun = EXCLUDED.owner_share_sun,
        reward_sun = EXCLUDED.reward_sun,
        owner_part_sun = EXCLUDED.owner_part_sun,
        level = EXCLUDED.level,
        allocated_at = EXCLUDED.allocated_at,
        allocation_at = EXCLUDED.allocation_at,
        updated_at = NOW()
    `,
    [
      normalizedPurchaseId,
      normalizedAllocationTxHash,
      payload.buyerWallet,
      payload.ambassadorWallet,
      payload.purchaseAmountSun,
      payload.ownerShareSun,
      payload.rewardSun,
      payload.ownerPartSun,
      Number(payload.level || 0),
      allocationAt
    ]
  );

  const updateResult = await client.query(
    `
      UPDATE purchases
      SET
        resolved_ambassador_wallet = $2,
        controller_processed = TRUE,
        controller_processed_tx_hash = $3,
        controller_processed_at = $4,
        controller_reward_sun = $5,
        controller_owner_part_sun = $6,
        controller_level = $7,
        processing_error = NULL,
        status = 'processed',
        updated_at = NOW()
      WHERE resolved_ambassador_wallet = $8
        AND (
          REPLACE(LOWER(purchase_id), '0x', '') = $1
          OR LOWER(tx_hash) = $9
        )
    `,
    [
      normalizedPurchaseId,
      payload.ambassadorWallet,
      normalizedAllocationTxHash,
      allocationAt,
      payload.rewardSun,
      payload.ownerPartSun,
      Number(payload.level || 0),
      payload.ambassadorWallet,
      normalizedPurchaseTxHash
    ]
  );

  if (updateResult.rowCount < 1) {
    throw new Error('Pending purchase row was not found for allocation update');
  }

  return {
    ok: true,
    rowCount: updateResult.rowCount
  };
}

module.exports = {
  getPurchaseByTxHash,
  upsertCandidatePurchase,
  upsertPurchaseFromTokenEvent,
  upsertReconciledPurchase,
  markPurchaseProcessed,
  markPurchaseError,
  recomputePurchaseStatuses,
  getReplayablePendingPurchases,
  applyAllocationResult
};
