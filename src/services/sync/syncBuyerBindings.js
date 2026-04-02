const { pool } = require('../../db/pool');
const { upsertBuyer } = require('../../db/queries/buyers');
const { getBuyerAmbassador } = require('../tron/controller');

async function syncBuyerBindings(limit = 100) {
  const result = await pool.query(
    `
      SELECT DISTINCT ON (buyer_wallet)
        buyer_wallet,
        tx_hash,
        token_block_time,
        id
      FROM purchases
      WHERE buyer_wallet IS NOT NULL
      ORDER BY buyer_wallet, token_block_time DESC NULLS LAST, id DESC
      LIMIT $1
    `,
    [limit]
  );

  const rows = result.rows || [];
  const synced = [];

  for (const row of rows) {
    try {
      const boundAmbassadorWallet = await getBuyerAmbassador(row.buyer_wallet);

      await upsertBuyer({
        buyerWallet: row.buyer_wallet,
        boundAmbassadorWallet: boundAmbassadorWallet || null,
        txHash: row.tx_hash,
        blockTime: row.token_block_time
      });

      synced.push({
        ok: true,
        buyerWallet: row.buyer_wallet,
        boundAmbassadorWallet: boundAmbassadorWallet || null
      });
    } catch (error) {
      synced.push({
        ok: false,
        buyerWallet: row.buyer_wallet,
        error: error.message
      });
    }
  }

  return {
    ok: true,
    processedCount: synced.length,
    results: synced
  };
}

module.exports = {
  syncBuyerBindings
};
