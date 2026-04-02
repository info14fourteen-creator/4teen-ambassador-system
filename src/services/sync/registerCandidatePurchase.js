const { upsertCandidatePurchase } = require('../../db/queries/purchases');
const { makePurchaseId } = require('../../utils/hashing');

async function registerCandidatePurchase({
  txHash,
  buyerWallet,
  candidateSlugHash,
  candidateAmbassadorWallet
}) {
  const purchaseId = makePurchaseId(txHash, buyerWallet);

  await upsertCandidatePurchase({
    txHash,
    purchaseId,
    buyerWallet,
    candidateSlugHash: candidateSlugHash || null,
    candidateAmbassadorWallet: candidateAmbassadorWallet || null
  });

  return {
    txHash,
    purchaseId,
    buyerWallet
  };
}

module.exports = {
  registerCandidatePurchase
};
