const express = require('express');
const { requireAdminAuth } = require('../services/security/adminAuth');
const { reconcilePurchase } = require('../services/sync/reconcilePurchase');
const { syncAmbassador } = require('../services/sync/syncAmbassador');
const { syncPurchasesRange } = require('../services/sync/syncPurchasesRange');
const { syncBuyerBindings } = require('../services/sync/syncBuyerBindings');

const router = express.Router();

router.use(requireAdminAuth);

router.post('/sync-purchase', async (req, res) => {
  try {
    const { txHash } = req.body || {};

    if (!txHash) {
      return res.status(400).json({
        ok: false,
        error: 'txHash is required'
      });
    }

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

router.post('/sync-ambassador', async (req, res) => {
  try {
    const { ambassadorWallet } = req.body || {};

    if (!ambassadorWallet) {
      return res.status(400).json({
        ok: false,
        error: 'ambassadorWallet is required'
      });
    }

    const result = await syncAmbassador(ambassadorWallet);

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

router.post('/sync-purchases-range', async (req, res) => {
  try {
    const { limit, minBlockTimestamp, maxBlockTimestamp } = req.body || {};

    const result = await syncPurchasesRange({
      limit: Number(limit || 10),
      minBlockTimestamp: minBlockTimestamp ? Number(minBlockTimestamp) : undefined,
      maxBlockTimestamp: maxBlockTimestamp ? Number(maxBlockTimestamp) : undefined
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

router.post('/sync-buyer-bindings', async (req, res) => {
  try {
    const { limit } = req.body || {};
    const result = await syncBuyerBindings(Number(limit || 100));

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
