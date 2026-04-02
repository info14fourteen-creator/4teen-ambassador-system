const { pool } = require('../../db/pool');
const {
  getPurchaseByTxHash,
  upsertReconciledPurchase,
  markPurchaseError
} = require('../../db/queries/purchases');
const { upsertBuyer } = require('../../db/queries/buyers');
const { makePurchaseId } = require('../../utils/hashing');
const { getTransaction, parseBuyTransaction } = require('../tron/token');
const {
  getBuyerAmbassador,
  getAmbassadorBySlugHash,
  isPurchaseProcessed,
  recordVerifiedPurchase
} = require('../tron/controller');

async function reconcilePurchase(txHash) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const { tx, info } = await getTransaction(txHash);
    const parsed = parseBuyTransaction(tx, info);

    const purchaseId = makePurchaseId(txHash, parsed.buyerWallet);
    const existing = await getPurchaseByTxHash(txHash, client);

    let candidateSlugHash = existing?.candidate_slug_hash || null;
    let candidateAmbassadorWallet = existing?.candidate_ambassador_wallet || null;

    if (!candidateAmbassadorWallet && candidateSlugHash) {
      const resolvedFromSlug = await getAmbassadorBySlugHash(candidateSlugHash);
      if (resolvedFromSlug) {
        candidateAmbassadorWallet = resolvedFromSlug;
      }
    }

    let boundAmbassadorWallet = await getBuyerAmbassador(parsed.buyerWallet);
    const alreadyProcessed = await isPurchaseProcessed(purchaseId);
    let controllerTxHash = null;

    if (!alreadyProcessed && !boundAmbassadorWallet && candidateAmbassadorWallet) {
      controllerTxHash = await recordVerifiedPurchase({
        purchaseId,
        buyerWallet: parsed.buyerWallet,
        ambassadorCandidate: candidateAmbassadorWallet,
        purchaseAmountSun: parsed.purchaseAmountSun,
        ownerShareSun: parsed.ownerShareSun
      });

      boundAmbassadorWallet = await getBuyerAmbassador(parsed.buyerWallet);
    }

    await upsertBuyer({
      buyerWallet: parsed.buyerWallet,
      boundAmbassadorWallet: boundAmbassadorWallet || null,
      txHash,
      blockTime: parsed.tokenBlockTime
    }, client);

    await upsertReconciledPurchase({
      txHash,
      purchaseId,
      buyerWallet: parsed.buyerWallet,
      purchaseAmountSun: parsed.purchaseAmountSun,
      ownerShareSun: parsed.ownerShareSun,
      tokenAmountRaw: null,
      tokenBlockNumber: parsed.tokenBlockNumber,
      tokenBlockTime: parsed.tokenBlockTime,
      candidateSlugHash,
      candidateAmbassadorWallet,
      resolvedAmbassadorWallet: boundAmbassadorWallet || null,
      controllerProcessed: Boolean(alreadyProcessed || controllerTxHash),
      controllerProcessedTxHash: controllerTxHash || null
    }, client);

    await client.query('COMMIT');

    return {
      ok: true,
      txHash,
      purchaseId,
      buyerWallet: parsed.buyerWallet,
      resolvedAmbassadorWallet: boundAmbassadorWallet || null,
      controllerProcessed: Boolean(alreadyProcessed || controllerTxHash),
      controllerTxHash: controllerTxHash || null
    };
  } catch (error) {
    await client.query('ROLLBACK');
    await markPurchaseError({
      txHash,
      errorMessage: error.message
    });

    throw error;
  } finally {
    client.release();
  }
}

module.exports = {
  reconcilePurchase
};
