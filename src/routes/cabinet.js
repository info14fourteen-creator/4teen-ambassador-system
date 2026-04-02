const express = require('express');
const {
  getAmbassadorSummary,
  listAmbassadorBuyers,
  listAmbassadorPurchases,
  listAmbassadorPendingPurchases
} = require('../db/queries/cabinet');

const router = express.Router();

function normalizeLimit(value, fallback = 50, max = 200) {
  const n = Number(value || fallback);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(n, max);
}

function normalizeOffset(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n < 0) return 0;
  return n;
}

router.get('/ambassador/:wallet/summary', async (req, res) => {
  try {
    const { wallet } = req.params;
    const summary = await getAmbassadorSummary(wallet);

    if (!summary) {
      return res.status(404).json({
        ok: false,
        error: 'Ambassador not found'
      });
    }

    return res.json({
      ok: true,
      summary
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message
    });
  }
});

router.get('/ambassador/:wallet/buyers', async (req, res) => {
  try {
    const { wallet } = req.params;
    const limit = normalizeLimit(req.query.limit, 50, 200);
    const offset = normalizeOffset(req.query.offset);

    const result = await listAmbassadorBuyers(wallet, limit, offset);

    return res.json({
      ok: true,
      total: result.total,
      limit,
      offset,
      rows: result.rows
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message
    });
  }
});

router.get('/ambassador/:wallet/purchases', async (req, res) => {
  try {
    const { wallet } = req.params;
    const limit = normalizeLimit(req.query.limit, 50, 200);
    const offset = normalizeOffset(req.query.offset);
    const status = req.query.status ? String(req.query.status) : undefined;
    const buyerWallet = req.query.buyer_wallet ? String(req.query.buyer_wallet) : undefined;

    const result = await listAmbassadorPurchases({
      ambassadorWallet: wallet,
      status,
      buyerWallet,
      limit,
      offset
    });

    return res.json({
      ok: true,
      total: result.total,
      limit,
      offset,
      rows: result.rows
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message
    });
  }
});

router.get('/ambassador/:wallet/pending', async (req, res) => {
  try {
    const { wallet } = req.params;
    const limit = normalizeLimit(req.query.limit, 50, 200);
    const offset = normalizeOffset(req.query.offset);

    const result = await listAmbassadorPendingPurchases(wallet, limit, offset);

    return res.json({
      ok: true,
      total: result.total,
      limit,
      offset,
      rows: result.rows
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message
    });
  }
});

module.exports = router;
