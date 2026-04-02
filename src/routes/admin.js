const express = require('express');
const { requireAdminAuth } = require('../services/security/adminAuth');
const { syncAmbassador } = require('../services/sync/syncAmbassador');
const { syncTokenPurchases } = require('../services/sync/syncTokenPurchases');
const { syncControllerEvents } = require('../services/sync/syncControllerEvents');
const { rebuildAmbassadorBuyers } = require('../services/sync/rebuildAmbassadorBuyers');

const router = express.Router();

router.use(requireAdminAuth);

router.post('/sync-ambassador', async (req, res) => {
  try {
    const { ambassadorWallet } = req.body || {};

    if (!ambassadorWallet) {
      return res.status(400).json({ ok: false, error: 'ambassadorWallet is required' });
    }

    const result = await syncAmbassador(ambassadorWallet);
    return res.json({ ok: true, result });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});

router.post('/sync-token-purchases', async (req, res) => {
  try {
    const { limit, minBlockTimestamp, maxBlockTimestamp } = req.body || {};

    const result = await syncTokenPurchases({
      limit: Number(limit || 10),
      minBlockTimestamp: minBlockTimestamp ? Number(minBlockTimestamp) : undefined,
      maxBlockTimestamp: maxBlockTimestamp ? Number(maxBlockTimestamp) : undefined
    });

    return res.json({ ok: true, result });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});

router.post('/sync-purchases-range', async (req, res) => {
  try {
    const { limit, minBlockTimestamp, maxBlockTimestamp } = req.body || {};

    const result = await syncTokenPurchases({
      limit: Number(limit || 10),
      minBlockTimestamp: minBlockTimestamp ? Number(minBlockTimestamp) : undefined,
      maxBlockTimestamp: maxBlockTimestamp ? Number(maxBlockTimestamp) : undefined
    });

    return res.json({ ok: true, result });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});

router.post('/sync-controller-events', async (req, res) => {
  try {
    const { limit, minBlockTimestamp, maxBlockTimestamp } = req.body || {};

    const result = await syncControllerEvents({
      limit: Number(limit || 20),
      minBlockTimestamp: minBlockTimestamp ? Number(minBlockTimestamp) : undefined,
      maxBlockTimestamp: maxBlockTimestamp ? Number(maxBlockTimestamp) : undefined
    });

    return res.json({ ok: true, result });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});

router.post('/rebuild-ambassador-buyers', async (req, res) => {
  try {
    const result = await rebuildAmbassadorBuyers();
    return res.json({ ok: true, result });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});

module.exports = router;
