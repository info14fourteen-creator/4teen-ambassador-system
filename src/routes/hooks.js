const express = require('express');
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

    if (!txHash) {
      return res.status(400).json({
        ok: false,
        error: 'txHash is required'
      });
    }

    const result = await reconcilePurchase(txHash, {
      buyerWallet: buyerWallet || null,
      candidateSlugHash: candidateSlugHash || null,
      candidateAmbassadorWallet: candidateAmbassadorWallet || null
    });

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
