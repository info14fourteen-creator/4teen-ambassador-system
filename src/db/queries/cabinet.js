const { pool } = require('../pool');

async function getAmbassadorSummary(ambassadorWallet, client = pool) {
  const result = await client.query(
    `
      WITH purchase_counts AS (
        SELECT
          COUNT(*) FILTER (WHERE status = 'processed')::INTEGER AS processed_count,
          COUNT(*) FILTER (WHERE status = 'attributed')::INTEGER AS attributed_count,
          COUNT(*) FILTER (WHERE status = 'unattributed')::INTEGER AS unattributed_count
        FROM purchases
        WHERE resolved_ambassador_wallet = $1
      ),
      buyer_totals AS (
        SELECT
          COUNT(*)::INTEGER AS buyers_count,
          COALESCE(SUM(total_purchase_amount_sun), 0) AS buyers_total_purchase_amount_sun,
          COALESCE(SUM(processed_purchase_amount_sun), 0) AS buyers_processed_purchase_amount_sun
        FROM ambassador_buyers
        WHERE ambassador_wallet = $1
      )
      SELECT
        a.ambassador_wallet,
        a.exists_on_chain,
        a.active,
        a.self_registered,
        a.manual_assigned,
        a.override_enabled,
        a.current_level,
        a.override_level,
        a.effective_level,
        a.reward_percent,
        a.slug_hash,
        a.meta_hash,
        a.created_at_chain,
        a.total_buyers,
        a.total_volume_sun,
        a.total_rewards_accrued_sun,
        a.total_rewards_claimed_sun,
        a.claimable_rewards_sun,
        a.last_chain_sync_at,
        pc.processed_count,
        pc.attributed_count,
        pc.unattributed_count,
        bt.buyers_count,
        bt.buyers_total_purchase_amount_sun,
        bt.buyers_processed_purchase_amount_sun
      FROM ambassadors a
      CROSS JOIN purchase_counts pc
      CROSS JOIN buyer_totals bt
      WHERE a.ambassador_wallet = $1
      LIMIT 1
    `,
    [ambassadorWallet]
  );

  return result.rows[0] || null;
}

async function listAmbassadorBuyers(ambassadorWallet, limit = 50, offset = 0, client = pool) {
  const rowsResult = await client.query(
    `
      SELECT
        buyer_wallet,
        ambassador_wallet,
        binding_at,
        first_attributed_purchase_at,
        last_attributed_purchase_at,
        purchase_count,
        total_purchase_amount_sun,
        total_owner_share_sun,
        processed_purchase_count,
        processed_purchase_amount_sun,
        created_at,
        updated_at
      FROM ambassador_buyers
      WHERE ambassador_wallet = $1
      ORDER BY
        processed_purchase_amount_sun DESC,
        total_purchase_amount_sun DESC,
        last_attributed_purchase_at DESC NULLS LAST,
        buyer_wallet ASC
      LIMIT $2
      OFFSET $3
    `,
    [ambassadorWallet, limit, offset]
  );

  const countResult = await client.query(
    `
      SELECT COUNT(*)::INTEGER AS total
      FROM ambassador_buyers
      WHERE ambassador_wallet = $1
    `,
    [ambassadorWallet]
  );

  return {
    total: countResult.rows[0]?.total || 0,
    rows: rowsResult.rows
  };
}

async function listAmbassadorPurchases({
  ambassadorWallet,
  status,
  buyerWallet,
  limit = 50,
  offset = 0
}, client = pool) {
  const conditions = ['resolved_ambassador_wallet = $1'];
  const values = [ambassadorWallet];
  let idx = values.length + 1;

  if (status) {
    conditions.push(`status = $${idx}`);
    values.push(status);
    idx += 1;
  }

  if (buyerWallet) {
    conditions.push(`buyer_wallet = $${idx}`);
    values.push(buyerWallet);
    idx += 1;
  }

  const whereClause = conditions.join(' AND ');

  const rowsResult = await client.query(
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
      WHERE ${whereClause}
      ORDER BY token_block_time DESC NULLS LAST, id DESC
      LIMIT $${idx}
      OFFSET $${idx + 1}
    `,
    [...values, limit, offset]
  );

  const countResult = await client.query(
    `
      SELECT COUNT(*)::INTEGER AS total
      FROM purchases
      WHERE ${whereClause}
    `,
    values
  );

  return {
    total: countResult.rows[0]?.total || 0,
    rows: rowsResult.rows
  };
}

async function listAmbassadorPendingPurchases(ambassadorWallet, limit = 50, offset = 0, client = pool) {
  const rowsResult = await client.query(
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
        AND status = 'attributed'
        AND controller_processed = FALSE
      ORDER BY token_block_time DESC NULLS LAST, id DESC
      LIMIT $2
      OFFSET $3
    `,
    [ambassadorWallet, limit, offset]
  );

  const countResult = await client.query(
    `
      SELECT COUNT(*)::INTEGER AS total
      FROM purchases
      WHERE resolved_ambassador_wallet = $1
        AND status = 'attributed'
        AND controller_processed = FALSE
    `,
    [ambassadorWallet]
  );

  return {
    total: countResult.rows[0]?.total || 0,
    rows: rowsResult.rows
  };
}

module.exports = {
  getAmbassadorSummary,
  listAmbassadorBuyers,
  listAmbassadorPurchases,
  listAmbassadorPendingPurchases
};
