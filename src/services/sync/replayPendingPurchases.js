const { pool } = require('../../db/pool');
const {
  getReplayablePendingPurchases,
  applyAllocationResult
} = require('../../db/queries/purchases');
const { ensureOperatorResources } = require('../gasStation');
const {
  isPurchaseProcessed,
  recordVerifiedPurchase,
  getAllocationEventByTxHash
} = require('../tron/controller');
const { syncAmbassador } = require('./syncAmbassador');
const { rebuildAmbassadorBuyers } = require('./rebuildAmbassadorBuyers');

function clampLimit(value, fallback = 10, max = 50) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(Math.floor(parsed), max);
}

function normalizeWallet(value) {
  return String(value || '').trim();
}

function toErrorMessage(error) {
  if (error && typeof error.message === 'string' && error.message.trim()) {
    return error.message.trim();
  }

  return 'Unknown error';
}

function isResourceError(error) {
  const message = toErrorMessage(error).toLowerCase();

  return (
    message.includes('out_of_energy') ||
    message.includes('insufficient energy') ||
    message.includes('insufficient bandwidth') ||
    message.includes('resource insufficient') ||
    message.includes('account resource insufficient') ||
    message.includes('gas station') ||
    message.includes('timed out') ||
    message.includes('timeout')
  );
}

async function replayPendingPurchases({
  wallet,
  limit = 10,
  dryRun = false
}) {
  const ambassadorWallet = normalizeWallet(wallet);

  if (!ambassadorWallet) {
    throw new Error('wallet is required');
  }

  const normalizedLimit = clampLimit(limit);
  const pendingRows = await getReplayablePendingPurchases(ambassadorWallet, normalizedLimit);

  const result = {
    ok: true,
    wallet: ambassadorWallet,
    totalFound: pendingRows.length,
    attempted: 0,
    succeeded: 0,
    failed: 0,
    skipped: 0,
    stoppedReason: null,
    items: []
  };

  if (dryRun) {
    result.items = pendingRows.map((row) => ({
      ok: true,
      dryRun: true,
      id: String(row.id),
      txHash: row.tx_hash,
      purchaseId: row.purchase_id,
      buyerWallet: row.buyer_wallet,
      purchaseAmountSun: String(row.purchase_amount_sun || 0),
      ownerShareSun: String(row.owner_share_sun || 0),
      tokenBlockTime: row.token_block_time
        ? new Date(row.token_block_time).toISOString()
        : null,
      status: row.status
    }));

    return result;
  }

  for (const row of pendingRows) {
    const itemBase = {
      id: String(row.id),
      txHash: row.tx_hash,
      purchaseId: row.purchase_id,
      buyerWallet: row.buyer_wallet
    };

    try {
      const alreadyProcessedOnChain = await isPurchaseProcessed(row.purchase_id);

      if (alreadyProcessedOnChain) {
        result.skipped += 1;
        result.items.push({
          ok: true,
          skipped: true,
          reason: 'already_processed_on_chain',
          ...itemBase
        });
        continue;
      }

      await ensureOperatorResources();

      result.attempted += 1;

      const controllerTxHash = await recordVerifiedPurchase({
        purchaseId: row.purchase_id,
        buyerWallet: row.buyer_wallet,
        ambassadorCandidate: ambassadorWallet,
        purchaseAmountSun: row.purchase_amount_sun,
        ownerShareSun: row.owner_share_sun
      });

      const allocationEvent = await getAllocationEventByTxHash(controllerTxHash, {
        attempts: 8,
        delayMs: 1500
      });

      const client = await pool.connect();

      try {
        await client.query('BEGIN');

        await applyAllocationResult({
          purchaseId: allocationEvent.purchaseId,
          txHash: allocationEvent.txHash,
          purchaseTxHash: row.tx_hash,
          buyerWallet: allocationEvent.buyerWallet || row.buyer_wallet,
          ambassadorWallet: allocationEvent.ambassadorWallet || ambassadorWallet,
          purchaseAmountSun: allocationEvent.purchaseAmountSun || String(row.purchase_amount_sun || 0),
          ownerShareSun: allocationEvent.ownerShareSun || String(row.owner_share_sun || 0),
          rewardSun: allocationEvent.rewardSun,
          ownerPartSun: allocationEvent.ownerPartSun,
          level: allocationEvent.level,
          allocationAt: allocationEvent.blockTime
        }, client);

        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }

      result.succeeded += 1;
      result.items.push({
        ok: true,
        ...itemBase,
        controllerTxHash,
        rewardSun: allocationEvent.rewardSun,
        ownerPartSun: allocationEvent.ownerPartSun,
        level: allocationEvent.level,
        allocationAt: allocationEvent.blockTime
      });
    } catch (error) {
      const message = toErrorMessage(error);

      if (isResourceError(error)) {
        result.stoppedReason = 'resources_unavailable';
        result.items.push({
          ok: false,
          stopped: true,
          reason: message,
          ...itemBase
        });
        break;
      }

      result.failed += 1;
      result.items.push({
        ok: false,
        reason: message,
        ...itemBase
      });
    }
  }

  try {
    await syncAmbassador(ambassadorWallet);
  } catch (_) {}

  try {
    await rebuildAmbassadorBuyers();
  } catch (_) {}

  return result;
}

module.exports = {
  replayPendingPurchases
};
