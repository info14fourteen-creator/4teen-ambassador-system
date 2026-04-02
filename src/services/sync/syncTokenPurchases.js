const { tronWeb } = require('../tron/client');
const { getSyncState, setSyncState } = require('../../db/queries/syncState');
const { getPurchaseByTxHash, upsertPurchaseFromTokenEvent } = require('../../db/queries/purchases');
const { getBuyTokenEvents } = require('../tron/token');
const { makePurchaseId } = require('../../utils/hashing');

function toBase58Address(value) {
  if (!value) return null;

  try {
    if (typeof value === 'string' && value.startsWith('T')) return value;

    let hex = String(value).toLowerCase();
    if (hex.startsWith('0x')) hex = hex.slice(2);
    if (!hex.startsWith('41')) hex = `41${hex}`;

    return tronWeb.address.fromHex(hex);
  } catch (_) {
    return null;
  }
}

async function syncTokenPurchases({
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
      skippedExisting: 0,
      tokenBuyEventsLastTs: storedTs,
      results: []
    };
  }

  const results = [];
  let maxSeenTimestamp = storedTs;
  let skippedExisting = 0;

  for (const event of events) {
    const txHash = event?.transaction_id || null;
    const buyerWallet = toBase58Address(event?.result?.buyer || null);
    const eventTs = Number(event?.block_timestamp || 0);
    const purchaseAmountSun = String(event?.result?.amountTRX || 0);
    const tokenAmountRaw = String(event?.result?.amountTokens || 0);
    const tokenBlockNumber = Number(event?.block_number || 0);
    const tokenBlockTime = eventTs ? new Date(eventTs).toISOString() : null;

    if (!txHash || !buyerWallet) continue;

    if (eventTs > maxSeenTimestamp) {
      maxSeenTimestamp = eventTs;
    }

    const existingPurchase = await getPurchaseByTxHash(txHash);
    if (existingPurchase) {
      skippedExisting += 1;
      results.push({ ok: true, txHash, skipped: true, reason: 'already_exists' });
      continue;
    }

    const purchaseId = makePurchaseId(txHash, buyerWallet);
    const ownerShareSun = String(Math.floor(Number(purchaseAmountSun) * 0.07));

    await upsertPurchaseFromTokenEvent({
      txHash,
      purchaseId,
      buyerWallet,
      purchaseAmountSun,
      ownerShareSun,
      tokenAmountRaw,
      tokenBlockNumber,
      tokenBlockTime
    });

    results.push({
      ok: true,
      txHash,
      purchaseId,
      buyerWallet,
      tokenBlockTime
    });
  }

  if (maxSeenTimestamp > storedTs) {
    await setSyncState('token_buy_events_last_ts', String(maxSeenTimestamp));
  }

  await setSyncState('last_full_refresh_at', new Date().toISOString());

  return {
    ok: true,
    processedCount: results.filter(item => !item.skipped).length,
    skippedExisting,
    tokenBuyEventsLastTs: maxSeenTimestamp,
    results
  };
}

module.exports = {
  syncTokenPurchases
};
