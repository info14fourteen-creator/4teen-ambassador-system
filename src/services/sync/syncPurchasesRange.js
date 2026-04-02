const { getSyncState, setSyncState } = require('../../db/queries/syncState');
const { registerCandidatePurchase } = require('./registerCandidatePurchase');
const { reconcilePurchase } = require('./reconcilePurchase');
const { getBuyTokenEvents } = require('../tron/token');

async function syncPurchasesRange({
  limit = 10,
  minBlockTimestamp,
  maxBlockTimestamp
} = {}) {
  const now = Date.now();

  const storedTsRaw = await getSyncState('token_buy_events_last_ts', '0');
  const storedTs = Number(storedTsRaw || '0');

  const effectiveMinTs =
    typeof minBlockTimestamp === 'number' ? minBlockTimestamp : (storedTs > 0 ? storedTs + 1 : undefined);

  const effectiveMaxTs =
    typeof maxBlockTimestamp === 'number' ? maxBlockTimestamp : now;

  const batch = await getBuyTokenEvents({
    minBlockTimestamp: effectiveMinTs,
    maxBlockTimestamp: effectiveMaxTs,
    limit: Math.min(limit, 20)
  });

  if (!Array.isArray(batch) || batch.length === 0) {
    return {
      ok: true,
      processedCount: 0,
      tokenBuyEventsLastTs: storedTs,
      results: []
    };
  }

  const results = [];
  let maxSeenTimestamp = storedTs;

  for (const event of batch) {
    const txHash = event?.transaction;
    const buyerWallet = event?.result?.buyer || event?.result?._buyer || null;
    const eventTs = Number(event?.timestamp || 0);

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
