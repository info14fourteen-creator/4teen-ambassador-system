const express = require('express');
const {
  getAmbassadorSummary,
  listAmbassadorBuyers,
  listAmbassadorPurchases,
  listAmbassadorPendingPurchases
} = require('../db/queries/cabinet');

const router = express.Router();

function normalizeWallet(value) {
  return String(value || '').trim();
}

router.get('/ambassador/:wallet/summary', async (req, res) => {
  try {
    const wallet = normalizeWallet(req.params.wallet);

    if (!wallet) {
      return res.status(400).json({
        ok: false,
        error: 'wallet is required'
      });
    }

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
    const wallet = normalizeWallet(req.params.wallet);

    if (!wallet) {
      return res.status(400).json({
        ok: false,
        error: 'wallet is required'
      });
    }

    const result = await listAmbassadorBuyers(
      wallet,
      req.query.limit,
      req.query.offset
    );

    return res.json({
      ok: true,
      total: result.total,
      limit: Number(req.query.limit || 50),
      offset: Number(req.query.offset || 0),
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
    const wallet = normalizeWallet(req.params.wallet);

    if (!wallet) {
      return res.status(400).json({
        ok: false,
        error: 'wallet is required'
      });
    }

    const result = await listAmbassadorPurchases(wallet, {
      limit: req.query.limit,
      offset: req.query.offset,
      status: req.query.status ? String(req.query.status).trim() : ''
    });

    return res.json({
      ok: true,
      total: result.total,
      limit: Number(req.query.limit || 50),
      offset: Number(req.query.offset || 0),
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
    const wallet = normalizeWallet(req.params.wallet);

    if (!wallet) {
      return res.status(400).json({
        ok: false,
        error: 'wallet is required'
      });
    }

    const result = await listAmbassadorPendingPurchases(wallet, {
      limit: req.query.limit,
      offset: req.query.offset
    });

    return res.json({
      ok: true,
      total: result.total,
      limit: Number(req.query.limit || 50),
      offset: Number(req.query.offset || 0),
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
