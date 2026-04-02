const { pool } = require('../../db/pool');
const { recomputePurchaseStatuses } = require('../../db/queries/purchases');

async function rebuildAmbassadorBuyers() {
  await recomputePurchaseStatuses();

  await pool.query('TRUNCATE TABLE ambassador_buyers');

  await pool.query(
    `
      WITH purchase_binding AS (
        SELECT
          p.id,
          p.buyer_wallet,
          COALESCE(
            p.resolved_ambassador_wallet,
            cb.ambassador_wallet
          ) AS ambassador_wallet,
          cb.binding_at,
          p.token_block_time,
          p.purchase_amount_sun,
          p.owner_share_sun,
          p.controller_processed
        FROM purchases p
        LEFT JOIN (
          SELECT
            p2.id AS purchase_row_id,
            bb.ambassador_wallet,
            bb.binding_at,
            ROW_NUMBER() OVER (
              PARTITION BY p2.id
              ORDER BY bb.binding_at DESC, bb.id DESC
            ) AS rn
          FROM purchases p2
          JOIN buyer_bindings bb
            ON bb.buyer_wallet = p2.buyer_wallet
           AND bb.binding_at <= COALESCE(p2.token_block_time, NOW())
        ) cb
          ON cb.purchase_row_id = p.id
         AND cb.rn = 1
        WHERE p.buyer_wallet IS NOT NULL
      )
      INSERT INTO ambassador_buyers (
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
      )
      SELECT
        buyer_wallet,
        ambassador_wallet,
        MIN(binding_at) AS binding_at,
        MIN(token_block_time) AS first_attributed_purchase_at,
        MAX(token_block_time) AS last_attributed_purchase_at,
        COUNT(*)::INTEGER AS purchase_count,
        COALESCE(SUM(purchase_amount_sun), 0) AS total_purchase_amount_sun,
        COALESCE(SUM(owner_share_sun), 0) AS total_owner_share_sun,
        COUNT(*) FILTER (WHERE controller_processed)::INTEGER AS processed_purchase_count,
        COALESCE(SUM(purchase_amount_sun) FILTER (WHERE controller_processed), 0) AS processed_purchase_amount_sun,
        NOW(),
        NOW()
      FROM purchase_binding
      WHERE ambassador_wallet IS NOT NULL
      GROUP BY buyer_wallet, ambassador_wallet
    `
  );

  const result = await pool.query(
    `SELECT COUNT(*)::INTEGER AS count FROM ambassador_buyers`
  );

  return {
    ok: true,
    rows: result.rows[0]?.count || 0
  };
}

module.exports = {
  rebuildAmbassadorBuyers
};
