const express = require('express');
const { requireAdminAuth } = require('../services/security/adminAuth');
const { syncAmbassador } = require('../services/sync/syncAmbassador');
const { syncTokenPurchases } = require('../services/sync/syncTokenPurchases');
const { syncControllerEvents } = require('../services/sync/syncControllerEvents');
const { rebuildAmbassadorBuyers } = require('../services/sync/rebuildAmbassadorBuyers');

const router = express.Router();

router.use(requireAdminAuth);

let refreshFullRunning = false;

async function runRefreshFull({ ambassadorWallet }) {
  const tokenResult = await syncTokenPurchases({ limit: 20 });
  const controllerResult = await syncControllerEvents({ limit: 20 });
  const rebuildResult = await rebuildAmbassadorBuyers();

  let ambassadorResult = null;
  if (ambassadorWallet) {
    ambassadorResult = await syncAmbassador(ambassadorWallet);
  }

  return {
    ok: true,
    tokenPurchases: tokenResult,
    controllerEvents: controllerResult,
    derivedState: rebuildResult,
    ambassador: ambassadorResult
  };
}

router.post('/refresh/full', async (req, res) => {
  if (refreshFullRunning) {
    return res.status(409).json({
      ok: false,
      already_running: true,
      error: 'refresh_full_already_running'
    });
  }

  try {
    refreshFullRunning = true;

    const { ambassadorWallet } = req.body || {};
    const result = await runRefreshFull({ ambassadorWallet });

    return res.json({
      ok: true,
      result
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message
    });
  } finally {
    refreshFullRunning = false;
  }
});

router.post('/refresh/ambassador', async (req, res) => {
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

router.post('/backfill/token-purchases', async (req, res) => {
  try {
    const { limit, minBlockTimestamp, maxBlockTimestamp } = req.body || {};

    const result = await syncTokenPurchases({
      limit: Number(limit || 20),
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

router.post('/backfill/controller-events', async (req, res) => {
  try {
    const { limit, minBlockTimestamp, maxBlockTimestamp } = req.body || {};

    const result = await syncControllerEvents({
      limit: Number(limit || 20),
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

router.post('/rebuild/derived-state', async (req, res) => {
  try {
    const result = await rebuildAmbassadorBuyers();

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
