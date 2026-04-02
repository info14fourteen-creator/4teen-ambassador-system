const { pool } = require('../pool');

function pick(input, snake, camel, fallback = null) {
  if (input == null || typeof input !== 'object') {
    return fallback;
  }

  if (input[snake] !== undefined) {
    return input[snake];
  }

  if (camel && input[camel] !== undefined) {
    return input[camel];
  }

  return fallback;
}

async function upsertAmbassador(input) {
  const result = await pool.query(
    `
      INSERT INTO ambassadors (
        ambassador_wallet,
        slug,
        exists_on_chain,
        active,
        self_registered,
        manual_assigned,
        override_enabled,
        current_level,
        override_level,
        effective_level,
        reward_percent,
        slug_hash,
        meta_hash,
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
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,NOW()
      )
      ON CONFLICT (ambassador_wallet)
      DO UPDATE SET
        slug = COALESCE(EXCLUDED.slug, ambassadors.slug),
        exists_on_chain = EXCLUDED.exists_on_chain,
        active = EXCLUDED.active,
        self_registered = EXCLUDED.self_registered,
        manual_assigned = EXCLUDED.manual_assigned,
        override_enabled = EXCLUDED.override_enabled,
        current_level = EXCLUDED.current_level,
        override_level = EXCLUDED.override_level,
        effective_level = EXCLUDED.effective_level,
        reward_percent = EXCLUDED.reward_percent,
        slug_hash = EXCLUDED.slug_hash,
        meta_hash = EXCLUDED.meta_hash,
        created_at_chain = EXCLUDED.created_at_chain,
        total_buyers = EXCLUDED.total_buyers,
        total_volume_sun = EXCLUDED.total_volume_sun,
        total_rewards_accrued_sun = EXCLUDED.total_rewards_accrued_sun,
        total_rewards_claimed_sun = EXCLUDED.total_rewards_claimed_sun,
        claimable_rewards_sun = EXCLUDED.claimable_rewards_sun,
        last_chain_sync_at = EXCLUDED.last_chain_sync_at,
        updated_at = NOW()
      RETURNING
        ambassador_wallet,
        slug,
        exists_on_chain,
        active,
        self_registered,
        manual_assigned,
        override_enabled,
        current_level,
        override_level,
        effective_level,
        reward_percent,
        slug_hash,
        meta_hash,
        created_at_chain,
        total_buyers,
        total_volume_sun,
        total_rewards_accrued_sun,
        total_rewards_claimed_sun,
        claimable_rewards_sun,
        last_chain_sync_at,
        updated_at
    `,
    [
      pick(input, 'ambassador_wallet', 'ambassadorWallet'),
      pick(input, 'slug', 'slug'),
      pick(input, 'exists_on_chain', 'existsOnChain', false),
      pick(input, 'active', 'active', false),
      pick(input, 'self_registered', 'selfRegistered', false),
      pick(input, 'manual_assigned', 'manualAssigned', false),
      pick(input, 'override_enabled', 'overrideEnabled', false),
      pick(input, 'current_level', 'currentLevel', 0),
      pick(input, 'override_level', 'overrideLevel', 0),
      pick(input, 'effective_level', 'effectiveLevel', 0),
      pick(input, 'reward_percent', 'rewardPercent', '0'),
      pick(input, 'slug_hash', 'slugHash'),
      pick(input, 'meta_hash', 'metaHash'),
      pick(input, 'created_at_chain', 'createdAtChain'),
      pick(input, 'total_buyers', 'totalBuyers', '0'),
      pick(input, 'total_volume_sun', 'totalVolumeSun', '0'),
      pick(input, 'total_rewards_accrued_sun', 'totalRewardsAccruedSun', '0'),
      pick(input, 'total_rewards_claimed_sun', 'totalRewardsClaimedSun', '0'),
      pick(input, 'claimable_rewards_sun', 'claimableRewardsSun', '0'),
      pick(input, 'last_chain_sync_at', 'lastChainSyncAt')
    ]
  );

  return result.rows[0];
}

async function getAmbassadorByWallet(ambassadorWallet) {
  const result = await pool.query(
    `
      SELECT
        ambassador_wallet,
        slug,
        exists_on_chain,
        active,
        self_registered,
        manual_assigned,
        override_enabled,
        current_level,
        override_level,
        effective_level,
        reward_percent,
        slug_hash,
        meta_hash,
        created_at_chain,
        total_buyers,
        total_volume_sun,
        total_rewards_accrued_sun,
        total_rewards_claimed_sun,
        claimable_rewards_sun,
        last_chain_sync_at,
        updated_at
      FROM ambassadors
      WHERE ambassador_wallet = $1
      LIMIT 1
    `,
    [ambassadorWallet]
  );

  return result.rows[0] || null;
}

async function setAmbassadorSlug(ambassadorWallet, slug) {
  const result = await pool.query(
    `
      UPDATE ambassadors
      SET slug = $2,
          updated_at = NOW()
      WHERE ambassador_wallet = $1
      RETURNING
        ambassador_wallet,
        slug,
        slug_hash,
        updated_at
    `,
    [ambassadorWallet, slug]
  );

  return result.rows[0] || null;
}

module.exports = {
  upsertAmbassador,
  getAmbassadorByWallet,
  setAmbassadorSlug
};
