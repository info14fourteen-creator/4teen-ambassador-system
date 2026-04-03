const { pool } = require('../../db/pool');
const { ensureOperatorResources } = require('../gasStation');
const {
  isPurchaseProcessed,
  recordVerifiedPurchase,
  getAllocationEventByTxHash
} = require('../tron/controller');
const { syncAmbassador } = require('./syncAmbassador');
const { rebuildAmbassadorBuyers } = require('./rebuildAmbassadorBuyers');

function normalizeWallet(value) {
  return String(value || '').trim();
}

function normalizeBytes32(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/^0x/, '');
}

function normalizeTxHash(value) {
  return String(value || '').trim().toLowerCase();
}

function toText(value, fallback = '0') {
  if (value == null) return fallback;
  const text = String(value).trim();
  return text || fallback;
}

function isResourceError(error) {
  const message = String(error?.message || '').toLowerCase();

  return (
    message.includes('out_of_energy') ||
    message.includes('resource') ||
    message.includes('energy') ||
    message.includes('bandwidth') ||
    message.includes('gas station') ||
    message.includes('gasstation') ||
    message.includes('timeout')
  );
}

async function getPendingPurchases(wallet, limit = 50) {
  const result = await pool.query(
    `
      SELECT
        id,
        tx_hash,
        purchase_id,
        buyer_wallet,
        resolved_ambassador_wallet,
        purchase_amount_sun,
        owner_share_sun,
        token_block_time,
        created_at,
        updated_at
      FROM purchases
      WHERE resolved_ambassador_wallet = $1
        AND status = 'attributed'
        AND controller_processed = FALSE
      ORDER BY
        token_block_time ASC NULLS LAST,
        created_at ASC,
        id ASC
      LIMIT $2
    `,
    [wallet, limit]
  );

  return result.rows;
}

async function applyAllocationEvent(event, purchaseRow) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    await client.query(
      `
        INSERT INTO controller_purchase_allocations (
          purchase_id,
          buyer_wallet,
          ambassador_wallet,
          tx_hash,
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
          buyer_wallet = EXCLUDED.buyer_wallet,
          ambassador_wallet = EXCLUDED.ambassador_wallet,
          tx_hash = EXCLUDED.tx_hash,
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
        normalizeBytes32(event.purchaseId),
        event.buyerWallet,
        event.ambassadorWallet,
        normalizeTxHash(event.txHash),
        toText(event.purchaseAmountSun),
        toText(event.ownerShareSun),
        toText(event.rewardSun),
        toText(event.ownerPartSun),
        Number(event.level || 0),
        event.blockTime
      ]
    );

    await client.query(
      `
        UPDATE purchases
        SET
          resolved_ambassador_wallet = $2,
          controller_processed = TRUE,
          controller_processed_tx_hash = COALESCE(controller_processed_tx_hash, $3),
          controller_processed_at = COALESCE(controller_processed_at, $4),
          controller_reward_sun = $5,
          controller_owner_part_sun = $6,
          controller_level = $7,
          status = 'processed',
          processing_error = NULL,
          updated_at = NOW()
        WHERE id = $1
           OR purchase_id = $8
           OR purchase_id = $9
           OR tx_hash = $10
           OR controller_processed_tx_hash = $3
      `,
      [
        purchaseRow.id,
        event.ambassadorWallet,
        normalizeTxHash(event.txHash),
        event.blockTime,
        toText(event.rewardSun),
        toText(event.ownerPartSun),
        Number(event.level || 0),
        normalizeBytes32(event.purchaseId),
        `0x${normalizeBytes32(event.purchaseId)}`,
        normalizeTxHash(purchaseRow.tx_hash)
      ]
    );

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

async function markPurchaseProcessedWithoutAllocationDetails(purchaseRow) {
  await pool.query(
    `
      UPDATE purchases
      SET
        controller_processed = TRUE,
        status = 'processed',
        updated_at = NOW()
      WHERE id = $1
    `,
    [purchaseRow.id]
  );
}

async function processPendingPurchasesForAmbassador(ambassadorWallet, options = {}) {
  const wallet = normalizeWallet(ambassadorWallet);

  if (!wallet) {
    throw new Error('wallet is required');
  }

  const limit = Math.max(1, Math.min(Number(options.limit || 50), 100));
  const rows = await getPendingPurchases(wallet, limit);

  const result = {
    ok: true,
    wallet,
    totalFound: rows.length,
    attempted: 0,
    succeeded: 0,
    failed: 0,
    skipped: 0,
    stoppedReason: null,
    items: []
  };

  for (const row of rows) {
    const purchaseIdRaw = String(row.purchase_id || '').trim();
    const purchaseIdNormalized = normalizeBytes32(purchaseIdRaw);
    const purchaseIdForContract = purchaseIdRaw.startsWith('0x')
      ? purchaseIdRaw
      : `0x${purchaseIdNormalized}`;

    try {
      result.attempted += 1;

      const alreadyProcessed = await isPurchaseProcessed(purchaseIdForContract);

      if (alreadyProcessed) {
        const event = await getAllocationEventByTxHash(row.controller_processed_tx_hash || row.tx_hash, {
          attempts: 2,
          delayMs: 300
        }).catch(() => null);

        if (event) {
          await applyAllocationEvent(event, row);
          result.succeeded += 1;
          result.items.push({
            ok: true,
            purchaseId: purchaseIdRaw,
            txHash: normalizeTxHash(row.tx_hash),
            mode: 'already_processed_synced',
            controllerTxHash: normalizeTxHash(event.txHash),
            rewardSun: toText(event.rewardSun),
            ownerPartSun: toText(event.ownerPartSun),
            level: Number(event.level || 0)
          });
        } else {
          await markPurchaseProcessedWithoutAllocationDetails(row);
          result.skipped += 1;
          result.items.push({
            ok: true,
            purchaseId: purchaseIdRaw,
            txHash: normalizeTxHash(row.tx_hash),
            mode: 'already_processed_no_event'
          });
        }

        continue;
      }

      await ensureOperatorResources();

      const controllerTxHash = await recordVerifiedPurchase({
        purchaseId: purchaseIdForContract,
        buyerWallet: row.buyer_wallet,
        ambassadorCandidate: wallet,
        purchaseAmountSun: row.purchase_amount_sun,
        ownerShareSun: row.owner_share_sun
      });

      const event = await getAllocationEventByTxHash(controllerTxHash, {
        attempts: 6,
        delayMs: 1000
      });

      await applyAllocationEvent(event, row);

      result.succeeded += 1;
      result.items.push({
        ok: true,
        purchaseId: purchaseIdRaw,
        txHash: normalizeTxHash(row.tx_hash),
        mode: 'allocated_now',
        controllerTxHash: normalizeTxHash(controllerTxHash),
        rewardSun: toText(event.rewardSun),
        ownerPartSun: toText(event.ownerPartSun),
        level: Number(event.level || 0)
      });
    } catch (error) {
      const message = String(error?.message || 'Unknown error');

      result.failed += 1;
      result.items.push({
        ok: false,
        purchaseId: purchaseIdRaw,
        txHash: normalizeTxHash(row.tx_hash),
        error: message
      });

      if (isResourceError(error)) {
        result.stoppedReason = 'resources_unavailable';
        break;
      }
    }
  }

  await syncAmbassador(wallet);
  await rebuildAmbassadorBuyers();

  return result;
}

module.exports = {
  processPendingPurchasesForAmbassador
};
