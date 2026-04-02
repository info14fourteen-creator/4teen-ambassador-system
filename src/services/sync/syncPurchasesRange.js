const { tronWeb } = require('../tron/client');
const { getSyncState, setSyncState } = require('../../db/queries/syncState');
const { registerCandidatePurchase } = require('./registerCandidatePurchase');
const { reconcilePurchase } = require('./reconcilePurchase');
const { getBuyTokenEvents } = require('../tron/token');

function toBase58Address(value) {
  if (!value) return null;

  try {
    if (typeof value === 'string' && value.startsWith('T')) {
      return value;
    }

    let hex = String(value).toLowerCase();

    if (hex.startsWith('0x')) {
      hex = hex.slice(2);
    }

    if (!hex.startsWith('41')) {
      hex = `41${hex}`;
    }

    return tronWeb.address.fromHex(hex);
  } catch (_) {
    return null;
  }
}

async function syncPurchasesRange({
  limit = 10,
  minBlockTimestamp,
  maxBlockTimestamp
} = {}) {
  const now = Date.now();

  const storedTsRaw = await getSyncState('token_buy_events_last_ts', '0');
  const storedTs = Number(storedTsRaw || '0');

  const effectiveMinTs =
    typeof minBlockTimestamp === 'number'
      ? minBlockTimestamp
      : (storedTs > 0 ? storedTs + 1 : undefined);

  const effectiveMaxTs =
    typeof maxBlockTimestamp === 'number'
      ? maxBlockTimestamp
      : now;

  const response = await getBuyTokenEvents({
    minBlockTimestamp: effectiveMinTs,
    maxBlockTimestamp: effectiveMaxTs,
    limit: Math.min(limit, 20)
  });

  const events = Array.isArray(response?.data) ? response.data : [];

  if (events.length === 0) {
    return {
      ok: true,
      processedCount: 0,
      tokenBuyEventsLastTs: storedTs,
      results: []
    };
  }

  const results = [];
  let maxSeenTimestamp = storedTs;

  for (const event of events) {
    const txHash = event?.transaction_id || null;
    const buyerWallet = toBase58Address(event?.result?.buyer || event?.result?._buyer || null);
    const eventTs = Number(event?.block_timestamp || 0);

    if (!txHash) {
      continue;
    }

    if (eventTs > maxSeenTimestamp) {
      maxSeenTimestamp = eventTs;
    }

    try {
      if (buyerWallet) {
        await registerCandidatePurchase({
          txHash,
          buyerWallet,
          candidateSlugHash: null,
          candidateAmbassadorWallet: null
        });
      }

      const result = await reconcilePurchase(txHash);
      results.push(result);
    } catch (error) {
      results.push({
        ok: false,
        txHash,
        error: error.message
      });
    }
  }

  if (maxSeenTimestamp > storedTs) {
    await setSyncState('token_buy_events_last_ts', String(maxSeenTimestamp));
  }

  await setSyncState('last_full_refresh_at', new Date().toISOString());

  return {
    ok: true,
    processedCount: results.length,
    tokenBuyEventsLastTs: maxSeenTimestamp,
    results
  };
}

module.exports = {
  syncPurchasesRange
};
