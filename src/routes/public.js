const express = require('express');
const {
  getAmbassadorByWallet,
  getAmbassadorBySlug,
  setAmbassadorSlug
} = require('../db/queries/ambassadors');
const { syncAmbassador } = require('../services/sync/syncAmbassador');

const router = express.Router();

const DEFAULT_REFERRAL_BASE = 'https://4teen.me/?ref=';

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
