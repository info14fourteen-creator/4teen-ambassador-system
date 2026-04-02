const { pool } = require('../../db/pool');
const {
  getPurchaseByTxHash,
  upsertCandidatePurchase,
  upsertPurchaseFromTokenEvent,
  upsertReconciledPurchase,
  markPurchaseError
} = require('../../db/queries/purchases');
const { upsertBuyer } = require('../../db/queries/buyers');
const { upsertBuyerBinding } = require('../../db/queries/buyerBindings');
const { makePurchaseId } = require('../../utils/hashing');
const { getBuyEventByTxHash } = require('../tron/token');
const {
  getBuyerAmbassador,
  getAmbassadorBySlugHash,
  isPurchaseProcessed,
  recordVerifiedPurchase
} = require('../tron/controller');
const { ensureOperatorResources } = require('../gasStation');
const { rebuildAmbassadorBuyers } = require('./rebuildAmbassadorBuyers');
const { syncAmbassador } = require('./syncAmbassador');

async function reconcilePurchase(txHash, options = {}) {
  const normalizedTxHash = String(txHash || '').toLowerCase();
  const existing = await getPurchaseByTxHash(normalizedTxHash);

  const candidateSlugHash =
    options.candidateSlugHash ||
    existing?.candidate_slug_hash ||
    null;

  let candidateAmbassadorWallet =
    options.candidateAmbassadorWallet ||
    existing?.candidate_ambassador_wallet ||
    null;

  const parsed = await getBuyEventByTxHash(normalizedTxHash);
  const purchaseId = makePurchaseId(normalizedTxHash, parsed.buyerWallet);

  await upsertCandidatePurchase({
    txHash: normalizedTxHash,
    purchaseId,
    buyerWallet: parsed.buyerWallet,
    candidateSlugHash,
    candidateAmbassadorWallet
  });

  if (!candidateAmbassadorWallet && candidateSlugHash) {
    candidateAmbassadorWallet = await getAmbassadorBySlugHash(candidateSlugHash);
  }

  let boundAmbassadorWallet = await getBuyerAmbassador(parsed.buyerWallet);
  const alreadyProcessed = await isPurchaseProcessed(purchaseId);

  let resourcePlan = {
    rented: false,
    before: null,
    after: null,
    orders: [],
    topUp: null
  };

  let controllerTxHash = null;

  if (!alreadyProcessed) {
    if (!boundAmbassadorWallet && !candidateAmbassadorWallet) {
      throw new Error('Buyer is not bound and ambassador candidate was not provided');
    }

    resourcePlan = await ensureOperatorResources();

    controllerTxHash = await recordVerifiedPurchase({
      purchaseId,
      buyerWallet: parsed.buyerWallet,
      ambassadorCandidate: boundAmbassadorWallet || candidateAmbassadorWallet,
      purchaseAmountSun: parsed.purchaseAmountSun,
      ownerShareSun: parsed.ownerShareSun
    });

    boundAmbassadorWallet = await getBuyerAmbassador(parsed.buyerWallet);
  }

  const resolvedAmbassadorWallet = boundAmbassadorWallet || candidateAmbassadorWallet || null;
  const bindingAtUsed = parsed.tokenBlockTime;

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    await upsertPurchaseFromTokenEvent({
      txHash: normalizedTxHash,
      purchaseId,
      buyerWallet: parsed.buyerWallet,
      purchaseAmountSun: parsed.purchaseAmountSun,
      ownerShareSun: parsed.ownerShareSun,
      tokenAmountRaw: parsed.tokenAmountRaw,
      tokenBlockNumber: parsed.tokenBlockNumber,
      tokenBlockTime: parsed.tokenBlockTime
    }, client);

    await upsertBuyer({
      buyerWallet: parsed.buyerWallet,
      boundAmbassadorWallet: resolvedAmbassadorWallet,
      txHash: normalizedTxHash,
      blockTime: parsed.tokenBlockTime
    }, client);

    if (controllerTxHash && resolvedAmbassadorWallet) {
      await upsertBuyerBinding({
        buyerWallet: parsed.buyerWallet,
        ambassadorWallet: resolvedAmbassadorWallet,
        oldAmbassadorWallet: null,
        bindingAt: parsed.tokenBlockTime,
        source: 'immediate_after_buy',
        eventName: 'BuyerBound',
        bindingTxHash: controllerTxHash
      }, client);
    }

    await upsertReconciledPurchase({
      txHash: normalizedTxHash,
      purchaseId,
      buyerWallet: parsed.buyerWallet,
      purchaseAmountSun: parsed.purchaseAmountSun,
      ownerShareSun: parsed.ownerShareSun,
      tokenAmountRaw: parsed.tokenAmountRaw,
      tokenBlockNumber: parsed.tokenBlockNumber,
      tokenBlockTime: parsed.tokenBlockTime,
      candidateSlugHash,
      candidateAmbassadorWallet,
      resolvedAmbassadorWallet,
      controllerProcessed: Boolean(alreadyProcessed || controllerTxHash),
      controllerProcessedTxHash: controllerTxHash || null,
      controllerProcessedAt: controllerTxHash ? new Date().toISOString() : null,
      bindingAtUsed
    }, client);

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    await markPurchaseError({
      txHash: normalizedTxHash,
      errorMessage: error.message
    });
    throw error;
  } finally {
    client.release();
  }

  if (resolvedAmbassadorWallet) {
    try {
      await syncAmbassador(resolvedAmbassadorWallet);
    } catch (_) {}
  }

  try {
    await rebuildAmbassadorBuyers();
  } catch (_) {}

  return {
    ok: true,
    txHash: normalizedTxHash,
    purchaseId,
    buyerWallet: parsed.buyerWallet,
    resolvedAmbassadorWallet,
    controllerProcessed: Boolean(alreadyProcessed || controllerTxHash),
    controllerTxHash: controllerTxHash || null,
    resourcePlan
  };
}

module.exports = {
  reconcilePurchase
};
