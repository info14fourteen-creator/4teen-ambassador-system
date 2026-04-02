const { pool } = require('../pool');

function clampLimit(value, fallback = 50, max = 200) {
  const parsed = Number(value);

  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }

  return Math.min(Math.floor(parsed), max);
}

function clampOffset(value, fallback = 0) {
  const parsed = Number(value);

  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallback;
  }

  return Math.floor(parsed);
}

function toSunString(value) {
  if (value == null) return '0';
  const text = String(value).trim();
  return text || '0';
}

function addSunStrings(a, b) {
  return (BigInt(toSunString(a)) + BigInt(toSunString(b))).toString();
}

function rewardSunFromOwnerShare(ownerShareSun, rewardPercent) {
  const owner = BigInt(toSunString(ownerShareSun));
  const percent = BigInt(String(rewardPercent == null ? 0 : rewardPercent));
  return ((owner * percent) / 100n).toString();
}

function mapPurchaseRow(row, rewardPercent) {
  return {
    id: String(row.id),
    tx_hash: row.tx_hash,
    purchase_id: row.purchase_id,
    buyer_wallet: row.buyer_wallet,
    purchase_amount_sun: toSunString(row.purchase_amount_sun),
    owner_share_sun: toSunString(row.owner_share_sun),
    ambassador_reward_sun: rewardSunFromOwnerShare(row.owner_share_sun, rewardPercent),
    reward_percent: String(rewardPercent),
    token_amount_raw: row.token_amount_raw == null ? null : String(row.token_amount_raw),
    token_block_number: row.token_block_number == null ? null : String(row.token_block_number),
    token_block_time: row.token_block_time ? new Date(row.token_block_time).toISOString() : null,
    resolved_ambassador_wallet: row.resolved_ambassador_wallet,
    controller_processed: Boolean(row.controller_processed),
    controller_processed_tx_hash: row.controller_processed_tx_hash,
    controller_processed_at: row.controller_processed_at
      ? new Date(row.controller_processed_at).toISOString()
      : null,
    status: row.status,
    binding_at_used: row.binding_at_used ? new Date(row.binding_at_used).toISOString() : null,
    created_at: row.created_at ? new Date(row.created_at).toISOString() : null,
    updated_at: row.updated_at ? new Date(row.updated_at).toISOString() : null
  };
}

async function getAmbassadorRow(ambassadorWallet) {
  const result = await pool.query(
    `
      SELECT *
      FROM ambassadors
      WHERE ambassador_wallet = $1
      LIMIT 1
    `,
    [ambassadorWallet]
  );

  return result.rows[0] || null;
}

async function getPurchasesForAmbassador(ambassadorWallet) {
  const result = await pool.query(
    `
      SELECT
        id,
        tx_hash,
        purchase_id,
        buyer_wallet,
        purchase_amount_sun,
        owner_share_sun,
        token_amount_raw,
        token_block_number,
        token_block_time,
        resolved_ambassador_wallet,
        controller_processed,
        controller_processed_tx_hash,
        controller_processed_at,
        status,
        binding_at_used,
        created_at,
        updated_at
      FROM purchases
      WHERE resolved_ambassador_wallet = $1
        AND status IN ('processed', 'attributed')
      ORDER BY token_block_time DESC NULLS LAST, id DESC
    `,
    [ambassadorWallet]
  );

  return result.rows;
}

async function getLatestBindingsForAmbassador(ambassadorWallet) {
  const result = await pool.query(
    `
      SELECT DISTINCT ON (buyer_wallet)
        buyer_wallet,
        ambassador_wallet,
        binding_at,
        source,
        created_at,
        updated_at
      FROM buyer_bindings
      WHERE ambassador_wallet = $1
      ORDER BY buyer_wallet, binding_at DESC NULLS LAST, created_at DESC
    `,
    [ambassadorWallet]
  );

  const map = new Map();

  for (const row of result.rows) {
    map.set(row.buyer_wallet, {
      buyer_wallet: row.buyer_wallet,
      ambassador_wallet: row.ambassador_wallet,
      binding_at: row.binding_at ? new Date(row.binding_at).toISOString() : null,
      source: row.source || null,
      created_at: row.created_at ? new Date(row.created_at).toISOString() : null,
      updated_at: row.updated_at ? new Date(row.updated_at).toISOString() : null
    });
  }

  return map;
}

async function getAmbassadorSummary(ambassadorWallet) {
  const ambassador = await getAmbassadorRow(ambassadorWallet);

  if (!ambassador) {
    return null;
  }

  const rewardPercent = Number(ambassador.reward_percent || 0);
  const purchases = await getPurchasesForAmbassador(ambassadorWallet);

  let processedCount = 0;
  let attributedCount = 0;
  let buyersTotalPurchaseAmountSun = '0';
  let buyersProcessedPurchaseAmountSun = '0';
  let buyersTotalRewardSun = '0';
  let buyersProcessedRewardSun = '0';
  let buyersPendingRewardSun = '0';

  const uniqueBuyers = new Set();

  for (const row of purchases) {
    uniqueBuyers.add(row.buyer_wallet);

    const purchaseAmountSun = toSunString(row.purchase_amount_sun);
    const rewardSun = rewardSunFromOwnerShare(row.owner_share_sun, rewardPercent);

    buyersTotalPurchaseAmountSun = addSunStrings(buyersTotalPurchaseAmountSun, purchaseAmountSun);
    buyersTotalRewardSun = addSunStrings(buyersTotalRewardSun, rewardSun);

    if (row.status === 'processed' || row.controller_processed) {
      processedCount += 1;
      buyersProcessedPurchaseAmountSun = addSunStrings(
        buyersProcessedPurchaseAmountSun,
        purchaseAmountSun
      );
      buyersProcessedRewardSun = addSunStrings(buyersProcessedRewardSun, rewardSun);
    } else if (row.status === 'attributed') {
      attributedCount += 1;
      buyersPendingRewardSun = addSunStrings(buyersPendingRewardSun, rewardSun);
    }
  }

  return {
    ambassador_wallet: ambassador.ambassador_wallet,
    slug: ambassador.slug || null,
    exists_on_chain: Boolean(ambassador.exists_on_chain),
    active: Boolean(ambassador.active),
    self_registered: Boolean(ambassador.self_registered),
    manual_assigned: Boolean(ambassador.manual_assigned),
    override_enabled: Boolean(ambassador.override_enabled),
    current_level: Number(ambassador.current_level || 0),
    override_level: Number(ambassador.override_level || 0),
    effective_level: Number(ambassador.effective_level || 0),
    reward_percent: String(ambassador.reward_percent || 0),
    slug_hash: ambassador.slug_hash || null,
    meta_hash: ambassador.meta_hash || null,
    created_at_chain: ambassador.created_at_chain == null ? null : String(ambassador.created_at_chain),
    total_buyers: String(ambassador.total_buyers || 0),
    total_volume_sun: String(ambassador.total_volume_sun || 0),
    total_rewards_accrued_sun: String(ambassador.total_rewards_accrued_sun || 0),
    total_rewards_claimed_sun: String(ambassador.total_rewards_claimed_sun || 0),
    claimable_rewards_sun: String(ambassador.claimable_rewards_sun || 0),
    last_chain_sync_at: ambassador.last_chain_sync_at
      ? new Date(ambassador.last_chain_sync_at).toISOString()
      : null,
    processed_count: processedCount,
    attributed_count: attributedCount,
    unattributed_count: 0,
    buyers_count: uniqueBuyers.size,
    buyers_total_purchase_amount_sun: buyersTotalPurchaseAmountSun,
    buyers_processed_purchase_amount_sun: buyersProcessedPurchaseAmountSun,
    buyers_total_reward_sun: buyersTotalRewardSun,
    buyers_processed_reward_sun: buyersProcessedRewardSun,
    buyers_pending_reward_sun: buyersPendingRewardSun
  };
}

async function listAmbassadorBuyers(ambassadorWallet, limit = 50, offset = 0) {
  const ambassador = await getAmbassadorRow(ambassadorWallet);

  if (!ambassador) {
    return {
      total: 0,
      rows: []
    };
  }

  const rewardPercent = Number(ambassador.reward_percent || 0);
  const purchases = await getPurchasesForAmbassador(ambassadorWallet);
  const latestBindings = await getLatestBindingsForAmbassador(ambassadorWallet);

  const grouped = new Map();

  for (const row of purchases) {
    const buyerWallet = row.buyer_wallet;
    const purchaseAmountSun = toSunString(row.purchase_amount_sun);
    const ownerShareSun = toSunString(row.owner_share_sun);
    const rewardSun = rewardSunFromOwnerShare(ownerShareSun, rewardPercent);
    const tokenBlockTime = row.token_block_time ? new Date(row.token_block_time).toISOString() : null;

    if (!grouped.has(buyerWallet)) {
      grouped.set(buyerWallet, {
        buyer_wallet: buyerWallet,
        ambassador_wallet: ambassadorWallet,
        binding_at: latestBindings.get(buyerWallet)?.binding_at || null,
        first_attributed_purchase_at: tokenBlockTime,
        last_attributed_purchase_at: tokenBlockTime,
        purchase_count: 0,
        total_purchase_amount_sun: '0',
        total_owner_share_sun: '0',
        total_reward_amount_sun: '0',
        processed_purchase_count: 0,
        processed_purchase_amount_sun: '0',
        processed_reward_amount_sun: '0',
        pending_purchase_count: 0,
        pending_reward_amount_sun: '0',
        created_at: latestBindings.get(buyerWallet)?.created_at || null,
        updated_at: latestBindings.get(buyerWallet)?.updated_at || null
      });
    }

    const item = grouped.get(buyerWallet);

    item.purchase_count += 1;
    item.total_purchase_amount_sun = addSunStrings(item.total_purchase_amount_sun, purchaseAmountSun);
    item.total_owner_share_sun = addSunStrings(item.total_owner_share_sun, ownerShareSun);
    item.total_reward_amount_sun = addSunStrings(item.total_reward_amount_sun, rewardSun);

    if (!item.first_attributed_purchase_at || (tokenBlockTime && tokenBlockTime < item.first_attributed_purchase_at)) {
      item.first_attributed_purchase_at = tokenBlockTime;
    }

    if (!item.last_attributed_purchase_at || (tokenBlockTime && tokenBlockTime > item.last_attributed_purchase_at)) {
      item.last_attributed_purchase_at = tokenBlockTime;
    }

    if (row.status === 'processed' || row.controller_processed) {
      item.processed_purchase_count += 1;
      item.processed_purchase_amount_sun = addSunStrings(
        item.processed_purchase_amount_sun,
        purchaseAmountSun
      );
      item.processed_reward_amount_sun = addSunStrings(
        item.processed_reward_amount_sun,
        rewardSun
      );
    } else {
      item.pending_purchase_count += 1;
      item.pending_reward_amount_sun = addSunStrings(
        item.pending_reward_amount_sun,
        rewardSun
      );
    }
  }

  const rows = Array.from(grouped.values()).sort((a, b) => {
    const left = a.last_attributed_purchase_at || '';
    const right = b.last_attributed_purchase_at || '';
    return right.localeCompare(left);
  });

  const normalizedLimit = clampLimit(limit);
  const normalizedOffset = clampOffset(offset);

  return {
    total: rows.length,
    rows: rows.slice(normalizedOffset, normalizedOffset + normalizedLimit)
  };
}

async function listAmbassadorPurchases(ambassadorWallet, options = {}) {
  const ambassador = await getAmbassadorRow(ambassadorWallet);

  if (!ambassador) {
    return {
      total: 0,
      rows: []
    };
  }

  const rewardPercent = Number(ambassador.reward_percent || 0);
  const limit = clampLimit(options.limit);
  const offset = clampOffset(options.offset);

  const params = [ambassadorWallet];
  const where = [`resolved_ambassador_wallet = $1`];

  if (options.status) {
    params.push(options.status);
    where.push(`status = $${params.length}`);
  } else {
    where.push(`status IN ('processed', 'attributed')`);
  }

  const totalResult = await pool.query(
    `
      SELECT COUNT(*) AS total
      FROM purchases
      WHERE ${where.join(' AND ')}
    `,
    params
  );

  params.push(limit);
  params.push(offset);

  const rowsResult = await pool.query(
    `
      SELECT
        id,
        tx_hash,
        purchase_id,
        buyer_wallet,
        purchase_amount_sun,
        owner_share_sun,
        token_amount_raw,
        token_block_number,
        token_block_time,
        resolved_ambassador_wallet,
        controller_processed,
        controller_processed_tx_hash,
        controller_processed_at,
        status,
        binding_at_used,
        created_at,
        updated_at
      FROM purchases
      WHERE ${where.join(' AND ')}
      ORDER BY token_block_time DESC NULLS LAST, id DESC
      LIMIT $${params.length - 1}
      OFFSET $${params.length}
    `,
    params
  );

  return {
    total: Number(totalResult.rows[0]?.total || 0),
    rows: rowsResult.rows.map((row) => mapPurchaseRow(row, rewardPercent))
  };
}

async function listAmbassadorPendingPurchases(ambassadorWallet, options = {}) {
  return listAmbassadorPurchases(ambassadorWallet, {
    ...options,
    status: 'attributed'
  });
}

module.exports = {
  getAmbassadorSummary,
  listAmbassadorBuyers,
  listAmbassadorPurchases,
  listAmbassadorPendingPurchases
};
