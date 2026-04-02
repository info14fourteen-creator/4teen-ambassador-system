const express = require('express');
const { registerCandidatePurchase } = require('../services/sync/registerCandidatePurchase');
const { reconcilePurchase } = require('../services/sync/reconcilePurchase');

const router = express.Router();

router.post('/after-buy', async (req, res) => {
  try {
    const {
      txHash,
      buyerWallet,
      candidateSlugHash,
      candidateAmbassadorWallet
    } = req.body || {};

    if (!txHash || !buyerWallet) {
      return res.status(400).json({
        ok: false,
        error: 'txHash and buyerWallet are required'
      });
    }

    await registerCandidatePurchase({
      txHash,
      buyerWallet,
      candidateSlugHash: candidateSlugHash || null,
      candidateAmbassadorWallet: candidateAmbassadorWallet || null
    });

    const result = await reconcilePurchase(txHash);

    return res.json({
      ok: true,
      result
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message
    });
  }
});

module.exports = router;
