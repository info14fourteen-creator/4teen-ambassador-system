const { pool } = require('../pool');

async function upsertControllerPurchaseAllocation(payload, client = pool) {
  await client.query(
    `
      INSERT INTO controller_purchase_allocations (
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
        created_at,
        updated_at
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW(),NOW())
      ON CONFLICT (purchase_id)
      DO UPDATE SET
        tx_hash = EXCLUDED.tx_hash,
        buyer_wallet = EXCLUDED.buyer_wallet,
        ambassador_wallet = EXCLUDED.ambassador_wallet,
        purchase_amount_sun = EXCLUDED.purchase_amount_sun,
        owner_share_sun = EXCLUDED.owner_share_sun,
        reward_sun = EXCLUDED.reward_sun,
        owner_part_sun = EXCLUDED.owner_part_sun,
        level = EXCLUDED.level,
        allocated_at = EXCLUDED.allocated_at,
        updated_at = NOW()
    `,
    [
      payload.purchaseId,
      payload.txHash,
      payload.buyerWallet,
      payload.ambassadorWallet,
      payload.purchaseAmountSun,
      payload.ownerShareSun,
      payload.rewardSun,
      payload.ownerPartSun,
      payload.level,
      payload.allocatedAt
    ]
  );
}

module.exports = {
  upsertControllerPurchaseAllocation
};
