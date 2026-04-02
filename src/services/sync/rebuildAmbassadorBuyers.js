const { pool } = require('../../db/pool');
const { recomputePurchaseStatuses } = require('../../db/queries/purchases');

async function rebuildAmbassadorBuyers() {
  await recomputePurchaseStatuses();

  await pool.query('TRUNCATE TABLE ambassador_buyers');

  await pool.query(
    `
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
        p.buyer_wallet,
        COALESCE(p.resolved_ambassador_wallet, chosen.ambassador_wallet) AS ambassador_wallet,
        MIN(chosen.binding_at) AS binding_at,
        MIN(p.token_block_time) AS first_attributed_purchase_at,
        MAX(p.token_block_time) AS last_attributed_purchase_at,
        COUNT(*)::INTEGER AS purchase_count,
        COALESCE(SUM(p.purchase_amount_sun), 0) AS total_purchase_amount_sun,
        COALESCE(SUM(p.owner_share_sun), 0) AS total_owner_share_sun,
        COUNT(*) FILTER (WHERE p.controller_processed)::INTEGER AS processed_purchase_count,
        COALESCE(SUM(p.purchase_amount_sun) FILTER (WHERE p.controller_processed), 0) AS processed_purchase_amount_sun,
        NOW(),
        NOW()
      FROM purchases p
      LEFT JOIN LATERAL (
        SELECT
          bb.ambassador_wallet,
          bb.binding_at
        FROM buyer_bindings bb
        WHERE bb.buyer_wallet = p.buyer_wallet
          AND bb.binding_at <= COALESCE(p.token_block_time, NOW())
        ORDER BY bb.binding_at DESC, bb.id DESC
        LIMIT 1
      ) AS chosen ON TRUE
      WHERE p.buyer_wallet IS NOT NULL
        AND COALESCE(p.resolved_ambassador_wallet, chosen.ambassador_wallet) IS NOT NULL
      GROUP BY
        p.buyer_wallet,
        COALESCE(p.resolved_ambassador_wallet, chosen.ambassador_wallet)
    `
  );

  const result = await pool.query(`SELECT COUNT(*)::INTEGER AS count FROM ambassador_buyers`);

  return {
    ok: true,
    rows: result.rows[0]?.count || 0
  };
}

module.exports = {
  rebuildAmbassadorBuyers
};
