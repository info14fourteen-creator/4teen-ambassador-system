const { pool } = require('../../db/pool');
const { syncAmbassador } = require('./syncAmbassador');
const { rebuildAmbassadorBuyers } = require('./rebuildAmbassadorBuyers');

function normalizeBytes32(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/^0x/, '');
}

function normalizeHash(value) {
  return String(value || '').trim().toLowerCase();
}

function toText(value, fallback = '0') {
  if (value == null) return fallback;
  const text = String(value).trim();
  return text || fallback;
}

async function loadBrokenRows(limit = 500) {
  const result = await pool.query(
    `
      SELECT
        p.id,
        p.tx_hash,
        p.purchase_id,
        p.buyer_wallet,
        p.resolved_ambassador_wallet,
        p.purchase_amount_sun,
        p.owner_share_sun,
        p.controller_processed_tx_hash,
        p.controller_processed_at,
        p.token_block_time,
        p.status
      FROM purchases p
      WHERE p.controller_processed = TRUE
        AND (
          p.controller_reward_sun IS NULL
          OR p.controller_owner_part_sun IS NULL
          OR p.controller_level IS NULL
        )
      ORDER BY p.token_block_time ASC NULLS LAST, p.id ASC
      LIMIT $1
    `,
    [limit]
  );

  return result.rows;
}

async function loadAllocationMaps() {
  const result = await pool.query(
    `
      SELECT
        purchase_id,
        tx_hash,
        buyer_wallet,
        ambassador_wallet,
        purchase_amount_sun,
        owner_share_sun,
        reward_sun,
        owner_part_sun,
        level,
        allocated_at,
        allocation_at
      FROM controller_purchase_allocations
    `
  );

  const byPurchaseId = new Map();
  const byTxHash = new Map();

  for (const row of result.rows) {
    const normalizedPurchaseId = normalizeBytes32(row.purchase_id);
    const normalizedTxHash = normalizeHash(row.tx_hash);

    if (normalizedPurchaseId) {
      byPurchaseId.set(normalizedPurchaseId, row);
    }

    if (normalizedTxHash) {
      if (!byTxHash.has(normalizedTxHash)) {
        byTxHash.set(normalizedTxHash, []);
      }
      byTxHash.get(normalizedTxHash).push(row);
    }
  }

  return {
    byPurchaseId,
    byTxHash
  };
}

function resolveAllocationForPurchase(purchaseRow, maps) {
  const normalizedPurchaseId = normalizeBytes32(purchaseRow.purchase_id);
  const normalizedControllerTxHash = normalizeHash(purchaseRow.controller_processed_tx_hash);

  if (normalizedPurchaseId && maps.byPurchaseId.has(normalizedPurchaseId)) {
    return {
      matchType: 'purchase_id',
      allocation: maps.byPurchaseId.get(normalizedPurchaseId)
    };
  }

  const candidates = normalizedControllerTxHash
    ? (maps.byTxHash.get(normalizedControllerTxHash) || [])
    : [];

  if (candidates.length === 1) {
    return {
      matchType: 'controller_processed_tx_hash',
      allocation: candidates[0]
    };
  }

  return {
    matchType: null,
    allocation: null
  };
}

async function updatePurchaseFromAllocation(purchaseId, allocation) {
  const result = await pool.query(
    `
      UPDATE purchases
      SET
        controller_reward_sun = $2,
        controller_owner_part_sun = $3,
        controller_level = $4,
        controller_processed_tx_hash = COALESCE(controller_processed_tx_hash, $5),
        controller_processed_at = COALESCE(controller_processed_at, $6, $7),
        updated_at = NOW()
      WHERE id = $1
      RETURNING
        id,
        resolved_ambassador_wallet
    `,
    [
      purchaseId,
      toText(allocation.reward_sun),
      toText(allocation.owner_part_sun),
      Number(allocation.level || 0),
      normalizeHash(allocation.tx_hash),
      allocation.allocation_at,
      allocation.allocated_at
    ]
  );

  return result.rows[0] || null;
}

async function repairProcessedPurchaseAllocations(options = {}) {
  const limit = Math.max(1, Math.min(Number(options.limit || 500), 2000));
  const dryRun = options.dryRun === true;

  const brokenRows = await loadBrokenRows(limit);
  const maps = await loadAllocationMaps();

  const result = {
    ok: true,
    dryRun,
    totalFound: brokenRows.length,
    repaired: 0,
    skippedNoMatch: 0,
    skippedAmbiguous: 0,
    touchedAmbassadors: [],
    items: []
  };

  const touchedAmbassadors = new Set();

  for (const row of brokenRows) {
    const normalizedControllerTxHash = normalizeHash(row.controller_processed_tx_hash);
    const txCandidates = normalizedControllerTxHash
      ? (maps.byTxHash.get(normalizedControllerTxHash) || [])
      : [];

    const resolved = resolveAllocationForPurchase(row, maps);

    if (!resolved.allocation) {
      if (txCandidates.length > 1) {
        result.skippedAmbiguous += 1;
        result.items.push({
          ok: false,
          id: row.id,
          purchaseId: row.purchase_id,
          txHash: row.tx_hash,
          controllerProcessedTxHash: row.controller_processed_tx_hash,
          reason: 'ambiguous_controller_tx_hash',
          candidateCount: txCandidates.length
        });
      } else {
        result.skippedNoMatch += 1;
        result.items.push({
          ok: false,
          id: row.id,
          purchaseId: row.purchase_id,
          txHash: row.tx_hash,
          controllerProcessedTxHash: row.controller_processed_tx_hash,
          reason: 'allocation_not_found'
        });
      }

      continue;
    }

    if (!dryRun) {
      const updated = await updatePurchaseFromAllocation(row.id, resolved.allocation);

      if (updated?.resolved_ambassador_wallet) {
        touchedAmbassadors.add(updated.resolved_ambassador_wallet);
      }
    } else if (row.resolved_ambassador_wallet) {
      touchedAmbassadors.add(row.resolved_ambassador_wallet);
    }

    result.repaired += 1;
    result.items.push({
      ok: true,
      id: row.id,
      purchaseId: row.purchase_id,
      txHash: row.tx_hash,
      matchType: resolved.matchType,
      allocationPurchaseId: resolved.allocation.purchase_id,
      allocationTxHash: resolved.allocation.tx_hash,
      rewardSun: toText(resolved.allocation.reward_sun),
      ownerPartSun: toText(resolved.allocation.owner_part_sun),
      level: Number(resolved.allocation.level || 0)
    });
  }

  const ambassadorList = Array.from(touchedAmbassadors);

  if (!dryRun) {
    for (const wallet of ambassadorList) {
      try {
        await syncAmbassador(wallet);
      } catch (_) {}
    }

    try {
      await rebuildAmbassadorBuyers();
    } catch (_) {}
  }

  result.touchedAmbassadors = ambassadorList;

  return result;
}

module.exports = {
  repairProcessedPurchaseAllocations
};
