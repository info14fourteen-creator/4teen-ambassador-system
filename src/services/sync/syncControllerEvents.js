const { tronWeb } = require('../tron/client');
const { getSyncState, setSyncState } = require('../../db/queries/syncState');
const { upsertBuyerBinding } = require('../../db/queries/buyerBindings');
const { upsertControllerPurchaseAllocation } = require('../../db/queries/controllerAllocations');
const { markPurchaseProcessed } = require('../../db/queries/purchases');
const { getControllerEvents } = require('../tron/controller');

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

async function syncBindingEvents({ limit = 20, minBlockTimestamp, maxBlockTimestamp } = {}) {
  const now = Date.now();
  const storedTsRaw = await getSyncState('controller_binding_events_last_ts', '0');
  const storedTs = Number(storedTsRaw || '0');

  const effectiveMinTs =
    typeof minBlockTimestamp === 'number'
      ? minBlockTimestamp
      : (storedTs > 0 ? storedTs + 1 : undefined);

  const effectiveMaxTs =
    typeof maxBlockTimestamp === 'number'
      ? maxBlockTimestamp
      : now;

  const buyerBound = await getControllerEvents('BuyerBound', {
    minBlockTimestamp: effectiveMinTs,
    maxBlockTimestamp: effectiveMaxTs,
    limit
  });

  const buyerRebound = await getControllerEvents('BuyerRebound', {
    minBlockTimestamp: effectiveMinTs,
    maxBlockTimestamp: effectiveMaxTs,
    limit
  });

  const all = [
    ...(Array.isArray(buyerBound?.data) ? buyerBound.data : []),
    ...(Array.isArray(buyerRebound?.data) ? buyerRebound.data : [])
  ].sort((a, b) => Number(a.block_timestamp || 0) - Number(b.block_timestamp || 0));

  let maxSeenTimestamp = storedTs;
  const results = [];

  for (const event of all) {
    const eventName = event?.event_name || '';
    const bindingAtTs = Number(event?.block_timestamp || 0);
    const bindingAt = new Date(bindingAtTs).toISOString();
    const bindingTxHash = event?.transaction_id || null;

    const buyerWallet = toBase58Address(event?.result?.buyer || event?.result?.['0']);
    const ambassadorWallet =
      eventName === 'BuyerRebound'
        ? toBase58Address(event?.result?.newAmbassador || event?.result?.['2'])
        : toBase58Address(event?.result?.ambassador || event?.result?.['1']);
    const oldAmbassadorWallet =
      eventName === 'BuyerRebound'
        ? toBase58Address(event?.result?.oldAmbassador || event?.result?.['1'])
        : null;

    if (!bindingTxHash || !buyerWallet || !ambassadorWallet) continue;

    if (bindingAtTs > maxSeenTimestamp) {
      maxSeenTimestamp = bindingAtTs;
    }

    await upsertBuyerBinding({
      buyerWallet,
      ambassadorWallet,
      oldAmbassadorWallet,
      bindingAt,
      source: 'controller_event',
      eventName,
      bindingTxHash
    });

    results.push({
      ok: true,
      eventName,
      buyerWallet,
      ambassadorWallet,
      bindingAt
    });
  }

  if (maxSeenTimestamp > storedTs) {
    await setSyncState('controller_binding_events_last_ts', String(maxSeenTimestamp));
  }

  return {
    ok: true,
    processedCount: results.length,
    controllerBindingEventsLastTs: maxSeenTimestamp,
    results
  };
}

async function syncAllocationEvents({ limit = 20, minBlockTimestamp, maxBlockTimestamp } = {}) {
  const now = Date.now();
  const storedTsRaw = await getSyncState('controller_allocation_events_last_ts', '0');
  const storedTs = Number(storedTsRaw || '0');

  const effectiveMinTs =
    typeof minBlockTimestamp === 'number'
      ? minBlockTimestamp
      : (storedTs > 0 ? storedTs + 1 : undefined);

  const effectiveMaxTs =
    typeof maxBlockTimestamp === 'number'
      ? maxBlockTimestamp
      : now;

  const response = await getControllerEvents('PurchaseFundsAllocated', {
    minBlockTimestamp: effectiveMinTs,
    maxBlockTimestamp: effectiveMaxTs,
    limit
  });

  const events = Array.isArray(response?.data) ? response.data : [];
  let maxSeenTimestamp = storedTs;
  const results = [];

  for (const event of events) {
    const allocatedAtTs = Number(event?.block_timestamp || 0);
    const allocatedAt = new Date(allocatedAtTs).toISOString();
    const txHash = event?.transaction_id || null;

    const purchaseId = String(event?.result?.purchaseId || event?.result?.['0'] || '');
    const buyerWallet = toBase58Address(event?.result?.buyer || event?.result?.['1']);
    const ambassadorWallet = toBase58Address(event?.result?.ambassador || event?.result?.['2']);
    const purchaseAmountSun = String(event?.result?.purchaseAmountSun || event?.result?.['3'] || 0);
    const ownerShareSun = String(event?.result?.ownerShareSun || event?.result?.['4'] || 0);
    const rewardSun = String(event?.result?.rewardSun || event?.result?.['5'] || 0);
    const ownerPartSun = String(event?.result?.ownerPartSun || event?.result?.['6'] || 0);
    const level = Number(event?.result?.level || event?.result?.['7'] || 0);

    if (!purchaseId || !txHash || !buyerWallet || !ambassadorWallet) continue;

    if (allocatedAtTs > maxSeenTimestamp) {
      maxSeenTimestamp = allocatedAtTs;
    }

    await upsertControllerPurchaseAllocation({
      purchaseId,
      txHash,
      buyerWallet,
      ambassadorWallet,
      purchaseAmountSun,
      ownerShareSun,
      rewardSun,
      ownerPartSun,
      level,
      allocatedAt
    });

    await markPurchaseProcessed({
      purchaseId,
      buyerWallet,
      ambassadorWallet,
      txHash,
      allocatedAt
    });

    results.push({
      ok: true,
      purchaseId,
      buyerWallet,
      ambassadorWallet,
      txHash
    });
  }

  if (maxSeenTimestamp > storedTs) {
    await setSyncState('controller_allocation_events_last_ts', String(maxSeenTimestamp));
  }

  return {
    ok: true,
    processedCount: results.length,
    controllerAllocationEventsLastTs: maxSeenTimestamp,
    results
  };
}

async function syncControllerEvents({ limit = 20, minBlockTimestamp, maxBlockTimestamp } = {}) {
  const bindings = await syncBindingEvents({ limit, minBlockTimestamp, maxBlockTimestamp });
  const allocations = await syncAllocationEvents({ limit, minBlockTimestamp, maxBlockTimestamp });

  return {
    ok: true,
    bindings,
    allocations
  };
}

module.exports = {
  syncControllerEvents
};
