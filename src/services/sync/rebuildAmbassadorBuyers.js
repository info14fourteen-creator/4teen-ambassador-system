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
        buyer_wallet,
        resolved_ambassador_wallet AS ambassador_wallet,
        MIN(binding_at_used) AS binding_at,
        MIN(token_block_time) AS first_attributed_purchase_at,
        MAX(token_block_time) AS last_attributed_purchase_at,
        COUNT(*)::INTEGER AS purchase_count,
        COALESCE(SUM(purchase_amount_sun), 0) AS total_purchase_amount_sun,
        COALESCE(SUM(owner_share_sun), 0) AS total_owner_share_sun,
        COUNT(*) FILTER (WHERE controller_processed)::INTEGER AS processed_purchase_count,
        COALESCE(SUM(purchase_amount_sun) FILTER (WHERE controller_processed), 0) AS processed_purchase_amount_sun,
        NOW(),
        NOW()
      FROM purchases
      WHERE resolved_ambassador_wallet IS NOT NULL
      GROUP BY buyer_wallet, resolved_ambassador_wallet
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
