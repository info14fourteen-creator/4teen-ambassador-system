const { pool } = require('../pool');

async function upsertAmbassador(payload, client = pool) {
  await client.query(
    `
      INSERT INTO ambassadors (
        ambassador_wallet,
        slug_hash,
        meta_hash,
        exists_on_chain,
        active,
        self_registered,
        manual_assigned,
        override_enabled,
        current_level,
        override_level,
        effective_level,
        reward_percent,
        created_at_chain,
        total_buyers,
        total_volume_sun,
        total_rewards_accrued_sun,
        total_rewards_claimed_sun,
        claimable_rewards_sun,
        last_chain_sync_at,
        updated_at
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
        $14,$15,$16,$17,$18,NOW(),NOW()
      )
      ON CONFLICT (ambassador_wallet)
      DO UPDATE SET
        slug_hash = EXCLUDED.slug_hash,
        meta_hash = EXCLUDED.meta_hash,
        exists_on_chain = EXCLUDED.exists_on_chain,
        active = EXCLUDED.active,
        self_registered = EXCLUDED.self_registered,
        manual_assigned = EXCLUDED.manual_assigned,
        override_enabled = EXCLUDED.override_enabled,
        current_level = EXCLUDED.current_level,
        override_level = EXCLUDED.override_level,
        effective_level = EXCLUDED.effective_level,
        reward_percent = EXCLUDED.reward_percent,
        created_at_chain = EXCLUDED.created_at_chain,
        total_buyers = EXCLUDED.total_buyers,
        total_volume_sun = EXCLUDED.total_volume_sun,
        total_rewards_accrued_sun = EXCLUDED.total_rewards_accrued_sun,
        total_rewards_claimed_sun = EXCLUDED.total_rewards_claimed_sun,
        claimable_rewards_sun = EXCLUDED.claimable_rewards_sun,
        last_chain_sync_at = NOW(),
        updated_at = NOW()
    `,
    [
      payload.ambassadorWallet,
      payload.slugHash || null,
      payload.metaHash || null,
      Boolean(payload.existsOnChain),
      Boolean(payload.active),
      Boolean(payload.selfRegistered),
      Boolean(payload.manualAssigned),
      Boolean(payload.overrideEnabled),
      Number(payload.currentLevel || 0),
      Number(payload.overrideLevel || 0),
      Number(payload.effectiveLevel || 0),
      String(payload.rewardPercent || 0),
      Number(payload.createdAtChain || 0),
      String(payload.totalBuyers || 0),
      String(payload.totalVolumeSun || 0),
      String(payload.totalRewardsAccruedSun || 0),
      String(payload.totalRewardsClaimedSun || 0),
      String(payload.claimableRewardsSun || 0)
    ]
  );
}

module.exports = {
  upsertAmbassador
};
