const { pool } = require('../../db/pool');
const { tronWeb } = require('../tron/client');
const env = require('../../config/env');

const CONTROLLER_CONTRACT = env.FOURTEEN_CONTROLLER_CONTRACT;

function toIsoFromMs(value) {
  if (value == null) return null;
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return new Date(n).toISOString();
}

function toText(value, fallback = '0') {
  if (value == null) return fallback;
  const text = String(value).trim();
  return text || fallback;
}

function normalizeAddress(value) {
  if (!value) return null;

  try {
    if (typeof value === 'string' && value.startsWith('41') && value.length === 42) {
      return tronWeb.address.fromHex(value);
    }

    if (typeof value === 'string' && value.startsWith('0x') && value.length === 42) {
      return tronWeb.address.fromHex(`41${value.slice(2)}`);
    }

    if (typeof value === 'string' && value.length >= 34 && value.startsWith('T')) {
      return value;
    }
  } catch (_) {}

  return value || null;
}

function getEventField(result, ...keys) {
  for (const key of keys) {
    if (result && result[key] != null) {
      return result[key];
    }
  }
  return null;
}

async function getStateValue(key, fallback = '0') {
  const result = await pool.query(
    `
      SELECT state_value
      FROM sync_state
      WHERE state_key = $1
      LIMIT 1
    `,
    [key]
  );

  return result.rows[0]?.state_value ?? fallback;
}

async function setStateValue(key, value) {
  await pool.query(
    `
      INSERT INTO sync_state (state_key, state_value, updated_at)
      VALUES ($1, $2, NOW())
      ON CONFLICT (state_key)
      DO UPDATE SET
        state_value = EXCLUDED.state_value,
        updated_at = NOW()
    `,
    [key, String(value)]
  );
}

async function upsertBuyerBinding(payload) {
  await pool.query(
    `
      INSERT INTO buyer_bindings (
        buyer_wallet,
        ambassador_wallet,
        old_ambassador_wallet,
        new_ambassador_wallet,
        binding_at,
        source,
        event_name,
        binding_tx_hash,
        created_at,
        updated_at
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,NOW(),NOW()
      )
    `,
    [
      payload.buyer_wallet,
      payload.ambassador_wallet,
      payload.old_ambassador_wallet,
      payload.new_ambassador_wallet,
      payload.binding_at,
      'controller_event',
      payload.event_name,
      payload.binding_tx_hash
    ]
  );
}

async function upsertAllocation(payload) {
  await pool.query(
    `
      INSERT INTO controller_purchase_allocations (
        purchase_id,
        buyer_wallet,
        ambassador_wallet,
        tx_hash,
        purchase_amount_sun,
        owner_share_sun,
        reward_sun,
        owner_part_sun,
        level,
        allocation_at,
        created_at,
        updated_at
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW(),NOW()
      )
      ON CONFLICT (purchase_id)
      DO UPDATE SET
        buyer_wallet = EXCLUDED.buyer_wallet,
        ambassador_wallet = EXCLUDED.ambassador_wallet,
        tx_hash = EXCLUDED.tx_hash,
        purchase_amount_sun = EXCLUDED.purchase_amount_sun,
        owner_share_sun = EXCLUDED.owner_share_sun,
        reward_sun = EXCLUDED.reward_sun,
        owner_part_sun = EXCLUDED.owner_part_sun,
        level = EXCLUDED.level,
        allocation_at = EXCLUDED.allocation_at,
        updated_at = NOW()
    `,
    [
      payload.purchase_id,
      payload.buyer_wallet,
      payload.ambassador_wallet,
      payload.tx_hash,
      payload.purchase_amount_sun,
      payload.owner_share_sun,
      payload.reward_sun,
      payload.owner_part_sun,
      payload.level,
      payload.allocation_at
    ]
  );

  await pool.query(
    `
      UPDATE purchases
      SET
        resolved_ambassador_wallet = $2,
        controller_processed = TRUE,
        controller_processed_tx_hash = $3,
        controller_processed_at = $4,
        controller_reward_sun = $5,
        controller_owner_part_sun = $6,
        controller_level = $7,
        status = 'processed',
        updated_at = NOW()
      WHERE purchase_id = $1
         OR tx_hash = $1
    `,
    [
      payload.purchase_id,
      payload.ambassador_wallet,
      payload.tx_hash,
      payload.allocation_at,
      payload.reward_sun,
      payload.owner_part_sun,
      payload.level
    ]
  );
}

async function insertWithdrawal(payload) {
  await pool.query(
    `
      INSERT INTO ambassador_reward_withdrawals (
        ambassador_wallet,
        amount_sun,
        tx_hash,
        block_time
      )
      VALUES ($1,$2,$3,$4)
      ON CONFLICT (tx_hash)
      DO NOTHING
    `,
    [
      payload.ambassador_wallet,
      payload.amount_sun,
      payload.tx_hash,
      payload.block_time
    ]
  );
}

async function fetchContractEvents({ eventName, limit, minBlockTimestamp, fingerprint }) {
  const options = {
    onlyConfirmed: true,
    orderBy: 'block_timestamp,asc',
    limit
  };

  if (eventName) {
    options.eventName = eventName;
  }

  if (minBlockTimestamp != null) {
    options.minBlockTimestamp = minBlockTimestamp;
  }

  if (fingerprint) {
    options.fingerprint = fingerprint;
  }

  return tronWeb.getEventResult(CONTROLLER_CONTRACT, options);
}

async function collectEvents(eventName, stateKey, limit, minBlockTimestamp, maxBlockTimestamp) {
  const stored = Number(await getStateValue(stateKey, '0'));
  const effectiveMin = Math.max(Number(minBlockTimestamp || 0), stored ? stored + 1 : 0);

  let fingerprint = null;
  let all = [];
  let maxSeen = stored;

  while (all.length < limit) {
    const page = await fetchContractEvents({
      eventName,
      limit: Math.min(200, limit - all.length),
      minBlockTimestamp: effectiveMin || undefined,
      fingerprint
    });

    const rows = Array.isArray(page?.data) ? page.data : Array.isArray(page) ? page : [];

    if (!rows.length) {
      break;
    }

    for (const row of rows) {
      const ts = Number(row.block_timestamp || 0);

      if (maxBlockTimestamp != null && ts > Number(maxBlockTimestamp)) {
        continue;
      }

      all.push(row);

      if (ts > maxSeen) {
        maxSeen = ts;
      }

      if (all.length >= limit) {
        break;
      }
    }

    fingerprint = page?.meta?.fingerprint || page?.fingerprint || null;

    if (!fingerprint || rows.length === 0 || all.length >= limit) {
      break;
    }
  }

  if (maxSeen > stored) {
    await setStateValue(stateKey, String(maxSeen));
  }

  return {
    rows: all,
    maxSeen
  };
}

async function syncBindingEvents({ limit, minBlockTimestamp, maxBlockTimestamp }) {
  const { rows } = await collectEvents(
    null,
    'controller_binding_events_last_ts',
    limit,
    minBlockTimestamp,
    maxBlockTimestamp
  );

  const filtered = rows.filter(
    (row) => row.event_name === 'BuyerBound' || row.event_name === 'BuyerRebound'
  );

  const results = [];

  for (const row of filtered) {
    const eventName = row.event_name;
    const result = row.result || {};
    const txHash = row.transaction_id;
    const bindingAt = toIsoFromMs(row.block_timestamp);

    if (eventName === 'BuyerBound') {
      const buyerWallet = normalizeAddress(getEventField(result, 'buyer', '0'));
      const ambassadorWallet = normalizeAddress(getEventField(result, 'ambassador', '1'));

      await upsertBuyerBinding({
        buyer_wallet: buyerWallet,
        ambassador_wallet: ambassadorWallet,
        old_ambassador_wallet: null,
        new_ambassador_wallet: ambassadorWallet,
        binding_at: bindingAt,
        event_name: 'BuyerBound',
        binding_tx_hash: txHash
      });

      results.push({
        ok: true,
        eventName: 'BuyerBound',
        buyerWallet,
        ambassadorWallet,
        bindingAt
      });
    }

    if (eventName === 'BuyerRebound') {
      const buyerWallet = normalizeAddress(getEventField(result, 'buyer', '0'));
      const oldAmbassadorWallet = normalizeAddress(getEventField(result, 'oldAmbassador', '1'));
      const newAmbassadorWallet = normalizeAddress(getEventField(result, 'newAmbassador', '2'));

      await upsertBuyerBinding({
        buyer_wallet: buyerWallet,
        ambassador_wallet: newAmbassadorWallet,
        old_ambassador_wallet: oldAmbassadorWallet,
        new_ambassador_wallet: newAmbassadorWallet,
        binding_at: bindingAt,
        event_name: 'BuyerRebound',
        binding_tx_hash: txHash
      });

      results.push({
        ok: true,
        eventName: 'BuyerRebound',
        buyerWallet,
        oldAmbassadorWallet,
        newAmbassadorWallet,
        bindingAt
      });
    }
  }

  return {
    ok: true,
    processedCount: results.length,
    results
  };
}

async function syncAllocationEvents({ limit, minBlockTimestamp, maxBlockTimestamp }) {
  const { rows } = await collectEvents(
    'PurchaseFundsAllocated',
    'controller_allocation_events_last_ts',
    limit,
    minBlockTimestamp,
    maxBlockTimestamp
  );

  const results = [];

  for (const row of rows) {
    const result = row.result || {};
    const purchaseIdRaw = getEventField(result, 'purchaseId', '0');
    const buyerWallet = normalizeAddress(getEventField(result, 'buyer', '1'));
    const ambassadorWallet = normalizeAddress(getEventField(result, 'ambassador', '2'));
    const purchaseAmountSun = toText(getEventField(result, 'purchaseAmountSun', '3'));
    const ownerShareSun = toText(getEventField(result, 'ownerShareSun', '4'));
    const rewardSun = toText(getEventField(result, 'rewardSun', '5'));
    const ownerPartSun = toText(getEventField(result, 'ownerPartSun', '6'));
    const level = Number(getEventField(result, 'level', '7') || 0);
    const txHash = row.transaction_id;
    const allocationAt = toIsoFromMs(row.block_timestamp);

    const purchaseId = String(purchaseIdRaw || '').replace(/^0x/i, '');

    await upsertAllocation({
      purchase_id: purchaseId,
      buyer_wallet: buyerWallet,
      ambassador_wallet: ambassadorWallet,
      tx_hash: txHash,
      purchase_amount_sun: purchaseAmountSun,
      owner_share_sun: ownerShareSun,
      reward_sun: rewardSun,
      owner_part_sun: ownerPartSun,
      level,
      allocation_at: allocationAt
    });

    results.push({
      ok: true,
      purchaseId,
      buyerWallet,
      ambassadorWallet,
      txHash,
      rewardSun,
      ownerPartSun,
      level
    });
  }

  return {
    ok: true,
    processedCount: results.length,
    results
  };
}

async function syncWithdrawalEvents({ limit, minBlockTimestamp, maxBlockTimestamp }) {
  const { rows } = await collectEvents(
    'RewardsWithdrawn',
    'controller_withdrawal_events_last_ts',
    limit,
    minBlockTimestamp,
    maxBlockTimestamp
  );

  const results = [];

  for (const row of rows) {
    const result = row.result || {};
    const ambassadorWallet = normalizeAddress(getEventField(result, 'ambassador', '0'));
    const amountSun = toText(getEventField(result, 'amountSun', '1'));
    const txHash = row.transaction_id;
    const blockTime = toIsoFromMs(row.block_timestamp);

    await insertWithdrawal({
      ambassador_wallet: ambassadorWallet,
      amount_sun: amountSun,
      tx_hash: txHash,
      block_time: blockTime
    });

    results.push({
      ok: true,
      ambassadorWallet,
      amountSun,
      txHash,
      blockTime
    });
  }

  return {
    ok: true,
    processedCount: results.length,
    results
  };
}

async function syncControllerEvents({
  limit = 100,
  minBlockTimestamp,
  maxBlockTimestamp
} = {}) {
  const normalizedLimit = Math.max(1, Math.min(Number(limit || 100), 500));

  const bindings = await syncBindingEvents({
    limit: normalizedLimit,
    minBlockTimestamp,
    maxBlockTimestamp
  });

  const allocations = await syncAllocationEvents({
    limit: normalizedLimit,
    minBlockTimestamp,
    maxBlockTimestamp
  });

  const withdrawals = await syncWithdrawalEvents({
    limit: normalizedLimit,
    minBlockTimestamp,
    maxBlockTimestamp
  });

  return {
    ok: true,
    bindings,
    allocations,
    withdrawals
  };
}

module.exports = {
  syncControllerEvents
};
