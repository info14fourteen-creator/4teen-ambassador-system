const express = require('express');
const env = require('../config/env');
const { pool } = require('../db/pool');
const {
  getAmbassadorByWallet,
  getAmbassadorBySlug,
  setAmbassadorSlug
} = require('../db/queries/ambassadors');
const { tronWeb } = require('../services/tron/client');
const { syncAmbassador } = require('../services/sync/syncAmbassador');
const { quoteEnergyRental, rentEnergyForWallet } = require('../services/gasStation');
const {
  getWalletSnapshot,
  getTrxPriceInfo
} = require('../services/public/walletSnapshot');

const router = express.Router();

const DEFAULT_REFERRAL_BASE = 'https://4teen.me/?ref=';
const SUN = 1_000_000;

function normalizeWallet(value) {
  return String(value || '').trim();
}

function normalizeSlug(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, '')
    .slice(0, 24);
}

function normalizeHash(value) {
  const text = String(value || '').trim();
  return text ? text.toLowerCase() : '';
}

function buildReferralLink(slug) {
  const normalizedSlug = normalizeSlug(slug);

  if (!normalizedSlug) {
    return '';
  }

  return `${DEFAULT_REFERRAL_BASE}${normalizedSlug}`;
}

function isValidTronAddress(value) {
  return /^T[1-9A-HJ-NP-Za-km-z]{33}$/.test(String(value || '').trim());
}

function normalizeTxid(value) {
  return String(value || '').trim().toLowerCase();
}

function toBase58Address(value) {
  if (!value) return '';

  try {
    return tronWeb.address.fromHex(String(value));
  } catch (_) {
    return '';
  }
}

function formatSunAsTrx(value) {
  const raw = String(value || '0');
  const whole = Math.floor(Number(raw) / SUN);
  const fraction = String(Math.floor(Number(raw) % SUN)).padStart(6, '0').replace(/0+$/, '');
  return fraction ? `${whole}.${fraction}` : String(whole);
}

function getRegistrationEnergyQuantity() {
  return Math.max(
    100000,
    Number(env.GASSTATION_REGISTRATION_ENERGY || 0),
    Number(env.GASSTATION_MIN_ENERGY || 0)
  );
}

async function buildRegistrationEnergyQuote() {
  const quote = await quoteEnergyRental({
    energyNum: getRegistrationEnergyQuantity()
  });

  return {
    paymentAddress: env.OPERATOR_WALLET,
    amountSun: String(Math.ceil(Number(quote.amountSun || 0))),
    amountTrx: formatSunAsTrx(Math.ceil(Number(quote.amountSun || 0))),
    energyQuantity: quote.energyQuantity
  };
}

async function assertRegistrationCandidate(wallet, slug) {
  const existingBySlug = await getAmbassadorBySlug(slug);

  if (existingBySlug && existingBySlug.ambassador_wallet !== wallet) {
    const error = new Error('Slug is already taken');
    error.status = 409;
    throw error;
  }

  let ambassador = await getAmbassadorByWallet(wallet);

  if (!ambassador || !ambassador.exists_on_chain) {
    try {
      await syncAmbassador(wallet);
    } catch (_) {}
    ambassador = await getAmbassadorByWallet(wallet);
  }

  if (ambassador && ambassador.exists_on_chain) {
    const error = new Error('Wallet is already registered as ambassador');
    error.status = 409;
    throw error;
  }
}

async function readTrxPayment(txid) {
  const tx = await tronWeb.trx.getTransaction(txid);
  const info = await tronWeb.trx.getTransactionInfo(txid).catch(() => null);
  const contract = tx?.raw_data?.contract?.[0];
  const value = contract?.parameter?.value || {};

  if (contract?.type !== 'TransferContract') {
    throw new Error('Payment transaction is not a TRX transfer');
  }

  if (info?.receipt?.result && info.receipt.result !== 'SUCCESS') {
    throw new Error('Payment transaction was not successful');
  }

  const owner = toBase58Address(value.owner_address);
  const recipient = toBase58Address(value.to_address);
  const amountSun = String(value.amount || '0');

  if (!owner || !recipient || !/^\d+$/.test(amountSun)) {
    throw new Error('Payment transaction is invalid');
  }

  return {
    txid,
    owner,
    recipient,
    amountSun
  };
}

router.get('/wallet/trx-price', async (_req, res) => {
  try {
    const result = await getTrxPriceInfo();
    return res.json(result);
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message
    });
  }
});

router.get('/wallet/snapshot', async (req, res) => {
  try {
    const address = normalizeWallet(req.query.address);

    if (!address) {
      return res.status(400).json({
        ok: false,
        error: 'address is required'
      });
    }

    if (!isValidTronAddress(address)) {
      return res.status(400).json({
        ok: false,
        error: 'invalid TRON address'
      });
    }

    const result = await getWalletSnapshot(address);
    return res.json(result);
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message
    });
  }
});

router.get('/slug/check', async (req, res) => {
  try {
    const slug = normalizeSlug(req.query.slug);

    if (!slug) {
      return res.status(400).json({
        ok: false,
        error: 'slug is required'
      });
    }

    const existing = await getAmbassadorBySlug(slug);

    return res.json({
      ok: true,
      slug,
      available: !existing
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message
    });
  }
});

router.get('/ambassador/by-wallet', async (req, res) => {
  try {
    const wallet = normalizeWallet(req.query.wallet);

    if (!wallet) {
      return res.status(400).json({
        ok: false,
        error: 'wallet is required'
      });
    }

    let ambassador = await getAmbassadorByWallet(wallet);

    if (!ambassador || !ambassador.exists_on_chain) {
      try {
        await syncAmbassador(wallet);
      } catch (_) {}
      ambassador = await getAmbassadorByWallet(wallet);
    }

    if (!ambassador || !ambassador.exists_on_chain) {
      return res.status(404).json({
        ok: false,
        error: 'Ambassador not found'
      });
    }

    return res.json({
      ok: true,
      registered: true,
      result: {
        wallet: ambassador.ambassador_wallet,
        slug: ambassador.slug || '',
        status: ambassador.active ? 'active' : 'inactive',
        referralLink: ambassador.slug ? buildReferralLink(ambassador.slug) : ''
      }
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message
    });
  }
});

router.post('/ambassador/registration-energy/quote', async (req, res) => {
  try {
    const wallet = normalizeWallet(req.body?.wallet);
    const slug = normalizeSlug(req.body?.slug);

    if (!wallet) {
      return res.status(400).json({
        ok: false,
        error: 'wallet is required'
      });
    }

    if (!isValidTronAddress(wallet)) {
      return res.status(400).json({
        ok: false,
        error: 'invalid TRON address'
      });
    }

    if (!slug) {
      return res.status(400).json({
        ok: false,
        error: 'slug is required'
      });
    }

    await assertRegistrationCandidate(wallet, slug);
    const quote = await buildRegistrationEnergyQuote();

    return res.json({
      ok: true,
      result: {
        wallet,
        slug,
        ...quote
      }
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      ok: false,
      error: error.message
    });
  }
});

router.post('/ambassador/registration-energy/confirm', async (req, res) => {
  try {
    const wallet = normalizeWallet(req.body?.wallet);
    const slug = normalizeSlug(req.body?.slug);
    const paymentTxid = normalizeTxid(req.body?.paymentTxId || req.body?.paymentTxHash || req.body?.txid);

    if (!wallet) {
      return res.status(400).json({
        ok: false,
        error: 'wallet is required'
      });
    }

    if (!isValidTronAddress(wallet)) {
      return res.status(400).json({
        ok: false,
        error: 'invalid TRON address'
      });
    }

    if (!slug) {
      return res.status(400).json({
        ok: false,
        error: 'slug is required'
      });
    }

    if (!paymentTxid) {
      return res.status(400).json({
        ok: false,
        error: 'paymentTxId is required'
      });
    }

    await assertRegistrationCandidate(wallet, slug);

    const existing = await pool.query(
      `
        SELECT *
        FROM ambassador_energy_rentals
        WHERE payment_tx_hash = $1
        LIMIT 1
      `,
      [paymentTxid]
    );

    if (existing.rows[0]?.status === 'completed') {
      return res.json({
        ok: true,
        result: existing.rows[0]
      });
    }

    if (existing.rows[0]) {
      return res.status(409).json({
        ok: false,
        error: 'Energy rental is already being processed for this payment'
      });
    }

    const quote = await buildRegistrationEnergyQuote();
    const payment = await readTrxPayment(paymentTxid);

    if (payment.owner !== wallet) {
      return res.status(400).json({
        ok: false,
        error: 'Payment sender does not match wallet'
      });
    }

    if (payment.recipient !== quote.paymentAddress) {
      return res.status(400).json({
        ok: false,
        error: 'Payment recipient does not match rental treasury'
      });
    }

    if (BigInt(payment.amountSun) < BigInt(quote.amountSun)) {
      return res.status(400).json({
        ok: false,
        error: 'Payment amount is lower than current rental quote'
      });
    }

    const inserted = await pool.query(
      `
        INSERT INTO ambassador_energy_rentals (
          wallet,
          slug,
          payment_tx_hash,
          payment_amount_sun,
          energy_quantity,
          status
        )
        VALUES ($1, $2, $3, $4, $5, 'paid')
        RETURNING *
      `,
      [wallet, slug, paymentTxid, payment.amountSun, quote.energyQuantity]
    );

    try {
      const rented = await rentEnergyForWallet({
        receiveAddress: wallet,
        energyNum: quote.energyQuantity,
        requestPrefix: 'amb-reg-energy'
      });

      const updated = await pool.query(
        `
          UPDATE ambassador_energy_rentals
          SET
            request_id = $2,
            trade_no = $3,
            status = 'completed',
            row_json = $4,
            updated_at = NOW()
          WHERE payment_tx_hash = $1
          RETURNING *
        `,
        [
          paymentTxid,
          rented.requestId,
          rented.tradeNo,
          JSON.stringify(rented.row || null)
        ]
      );

      return res.json({
        ok: true,
        result: updated.rows[0]
      });
    } catch (rentError) {
      await pool.query(
        `
          UPDATE ambassador_energy_rentals
          SET
            status = 'failed',
            updated_at = NOW()
          WHERE payment_tx_hash = $1
        `,
        [paymentTxid]
      );

      throw rentError;
    }
  } catch (error) {
    return res.status(error.status || 500).json({
      ok: false,
      error: error.message
    });
  }
});

router.post('/ambassador/register-complete', async (req, res) => {
  try {
    const wallet = normalizeWallet(req.body?.wallet);
    const slug = normalizeSlug(req.body?.slug);
    const requestedSlugHash = normalizeHash(req.body?.slugHash);

    if (!wallet) {
      return res.status(400).json({
        ok: false,
        error: 'wallet is required'
      });
    }

    if (!slug) {
      return res.status(400).json({
        ok: false,
        error: 'slug is required'
      });
    }

    const existingBySlug = await getAmbassadorBySlug(slug);

    if (existingBySlug && existingBySlug.ambassador_wallet !== wallet) {
      return res.status(409).json({
        ok: false,
        error: 'Slug is already taken'
      });
    }

    await syncAmbassador(wallet);

    const ambassador = await getAmbassadorByWallet(wallet);

    if (!ambassador || !ambassador.exists_on_chain) {
      return res.status(400).json({
        ok: false,
        error: 'Ambassador was not found on chain after registration'
      });
    }

    if (
      requestedSlugHash &&
      ambassador.slug_hash &&
      normalizeHash(ambassador.slug_hash) &&
      normalizeHash(ambassador.slug_hash) !== requestedSlugHash
    ) {
      return res.status(409).json({
        ok: false,
        error: 'slugHash mismatch with on-chain ambassador profile'
      });
    }

    const updated = await setAmbassadorSlug(wallet, slug);

    return res.json({
      ok: true,
      result: {
        wallet,
        slug: updated?.slug || slug,
        referralLink: buildReferralLink(updated?.slug || slug)
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
