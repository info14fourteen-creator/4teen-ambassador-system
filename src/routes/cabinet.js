const express = require('express');
const { pool } = require('../db/pool');
const {
  getAmbassadorSummary,
  listAmbassadorBuyers,
  listAmbassadorPurchases,
  listAmbassadorPendingPurchases
} = require('../db/queries/cabinet');
const { syncAmbassador } = require('../services/sync/syncAmbassador');
const { getWithdrawalEventByTxHash } = require('../services/tron/controller');

const router = express.Router();

function normalizeWallet(value) {
  return String(value || '').trim();
}

function normalizeTxid(value) {
  return String(value || '').trim().toLowerCase();
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

router.post('/confirm-withdrawal', async (req, res) => {
  try {
    const wallet = normalizeWallet(req.body?.wallet);
    const txid = normalizeTxid(req.body?.txid);

    if (!wallet) {
      return res.status(400).json({
        ok: false,
        error: 'wallet is required'
      });
    }

    if (!txid) {
      return res.status(400).json({
        ok: false,
        error: 'txid is required'
      });
    }

    const event = await getWithdrawalEventByTxHash(txid);

    if (event.ambassadorWallet !== wallet) {
      return res.status(400).json({
        ok: false,
        error: 'Withdrawal transaction does not belong to the provided wallet'
      });
    }

    const insertResult = await pool.query(
      `
        INSERT INTO ambassador_reward_withdrawals (
          ambassador_wallet,
          amount_sun,
          tx_hash,
          block_time
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (tx_hash)
        DO NOTHING
        RETURNING
          id,
          ambassador_wallet,
          amount_sun,
          tx_hash,
          block_time,
          created_at
      `,
      [
        event.ambassadorWallet,
        event.amountSun,
        event.txHash,
        event.blockTime
      ]
    );

    await syncAmbassador(wallet);

    return res.json({
      ok: true,
      result: {
        wallet,
        txid: event.txHash,
        amountSun: event.amountSun,
        blockTime: event.blockTime,
        inserted: insertResult.rowCount > 0
      }
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message
    });
  }
});

module.exports = router;
