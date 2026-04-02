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
const { getAmbassadorBySlug } = require('../../db/queries/ambassadors');
const { makePurchaseId } = require('../../utils/hashing');
const { getBuyEventByTxHash } = require('../tron/token');
const {
  getBuyerAmbassador,
  isPurchaseProcessed,
  recordVerifiedPurchase
} = require('../tron/controller');
const { ensureOperatorResources } = require('../gasStation');
const { rebuildAmbassadorBuyers } = require('./rebuildAmbassadorBuyers');
const { syncAmbassador } = require('./syncAmbassador');

function normalizeSlug(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, '')
    .slice(0, 24);
}

function isValidSlug(value) {
  return /^[a-z0-9_-]{3,24}$/.test(String(value || ''));
}

function toErrorMessage(error) {
  if (error && typeof error.message === 'string' && error.message.trim()) {
    return error.message.trim();
  }

  return 'Unknown error';
}

async function reconcilePurchase(txHash, options = {}) {
  const normalizedTxHash = String(txHash || '').toLowerCase();
  const incomingSlug = isValidSlug(normalizeSlug(options.slug))
    ? normalizeSlug(options.slug)
    : null;

  const existing = await getPurchaseByTxHash(normalizedTxHash);
  const parsed = await getBuyEventByTxHash(normalizedTxHash);
  const purchaseId = makePurchaseId(normalizedTxHash, parsed.buyerWallet);

  let candidateSlugHash = existing?.candidate_slug_hash || null;
  let candidateAmbassadorWallet = existing?.candidate_ambassador_wallet || null;

  if (!candidateAmbassadorWallet && incomingSlug) {
    const ambassador = await getAmbassadorBySlug(incomingSlug);

    if (ambassador?.ambassador_wallet) {
      candidateAmbassadorWallet = ambassador.ambassador_wallet;
      candidateSlugHash = ambassador.slug_hash || null;
    }
  }

  const alreadyBoundAmbassadorWallet = await getBuyerAmbassador(parsed.buyerWallet);

  let resolvedAmbassadorWallet = alreadyBoundAmbassadorWallet || null;
  let resolutionSource = alreadyBoundAmbassadorWallet ? 'controller_binding' : 'none';
  const alreadyProcessed = await isPurchaseProcessed(purchaseId);

  let resourcePlan = {
    rented: false,
    before: null,
    after: null,
    orders: [],
    topUp: null
  };

  let controllerTxHash = null;
  let finalStatus = 'detected';
  let processingError = null;
  let bindingAtUsed = null;

  if (alreadyProcessed) {
    finalStatus = 'processed';
  } else if (!alreadyBoundAmbassadorWallet && !candidateAmbassadorWallet) {
    finalStatus = 'awaiting_candidate';
    processingError = 'Buyer is not bound and referral slug was not provided or not resolved';
  } else {
    const ambassadorForAllocation = alreadyBoundAmbassadorWallet || candidateAmbassadorWallet;

    if (alreadyBoundAmbassadorWallet) {
      resolutionSource = 'controller_binding';
    } else {
      resolutionSource = 'incoming_slug';
    }

    try {
      resourcePlan = await ensureOperatorResources();

      controllerTxHash = await recordVerifiedPurchase({
        purchaseId,
        buyerWallet: parsed.buyerWallet,
        ambassadorCandidate: ambassadorForAllocation,
        purchaseAmountSun: parsed.purchaseAmountSun,
        ownerShareSun: parsed.ownerShareSun
      });

      resolvedAmbassadorWallet = await getBuyerAmbassador(parsed.buyerWallet) || ambassadorForAllocation;
      finalStatus = 'processed';
      bindingAtUsed = parsed.tokenBlockTime;
    } catch (error) {
      resolvedAmbassadorWallet = null;
      finalStatus = 'awaiting_resources';
      processingError = toErrorMessage(error);
    }
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    await upsertCandidatePurchase({
      txHash: normalizedTxHash,
      purchaseId,
      buyerWallet: parsed.buyerWallet,
      candidateSlugHash: candidateSlugHash || null,
      candidateAmbassadorWallet: candidateAmbassadorWallet || null
    }, client);

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
      boundAmbassadorWallet: resolvedAmbassadorWallet || null,
      txHash: normalizedTxHash,
      blockTime: parsed.tokenBlockTime
    }, client);

    if (controllerTxHash && resolvedAmbassadorWallet) {
      await upsertBuyerBinding({
        buyerWallet: parsed.buyerWallet,
        ambassadorWallet: resolvedAmbassadorWallet,
        oldAmbassadorWallet: null,
        bindingAt: parsed.tokenBlockTime,
        source: 'after_buy_allocation',
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
      candidateSlugHash: candidateSlugHash || null,
      candidateAmbassadorWallet: candidateAmbassadorWallet || null,
      resolvedAmbassadorWallet: resolvedAmbassadorWallet || null,
      controllerProcessed: Boolean(alreadyProcessed || controllerTxHash),
      controllerProcessedTxHash: controllerTxHash || null,
      controllerProcessedAt: controllerTxHash ? new Date().toISOString() : null,
      bindingAtUsed,
      status: finalStatus,
      processingError
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
    incomingSlug,
    candidateSlugHash: candidateSlugHash || null,
    candidateAmbassadorWallet: candidateAmbassadorWallet || null,
    resolvedAmbassadorWallet: resolvedAmbassadorWallet || null,
    resolutionSource,
    controllerProcessed: Boolean(alreadyProcessed || controllerTxHash),
    controllerTxHash: controllerTxHash || null,
    resourcePlan,
    status: finalStatus,
    processingError
  };
}

module.exports = {
  reconcilePurchase
};
