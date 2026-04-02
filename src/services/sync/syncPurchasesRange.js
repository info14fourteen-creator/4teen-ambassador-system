const env = require('../../config/env');
const { tronWeb } = require('../tron/client');
const { getSyncState, setSyncState } = require('../../db/queries/syncState');
const { reconcilePurchase } = require('./reconcilePurchase');

async function syncPurchasesRange(limit = 100) {
  const currentBlock = await tronWeb.trx.getCurrentBlock();
  const latestBlock = currentBlock?.block_header?.raw_data?.number || 0;
  let fromBlock = Number(await getSyncState('last_token_buy_block', '0'));

  const results = [];
  let processedCount = 0;

  while (fromBlock <= latestBlock && processedCount < limit) {
    const block = await tronWeb.trx.getBlock(fromBlock);
    const txs = Array.isArray(block?.transactions) ? block.transactions : [];

    for (const tx of txs) {
      if (processedCount >= limit) break;

      const txHash = tx?.txID;
      if (!txHash) continue;

      try {
        const contract = tx?.raw_data?.contract?.[0];
        const value = contract?.parameter?.value;
        const contractAddressHex = value?.contract_address;

        if (!contractAddressHex) continue;

        const contractAddress = tronWeb.address.fromHex(contractAddressHex);

        if (contractAddress !== env.FOURTEEN_TOKEN_CONTRACT) continue;
        if (value.call_value === undefined) continue;

        const result = await reconcilePurchase(txHash);
        results.push(result);
        processedCount += 1;
      } catch (_) {}
    }

    await setSyncState('last_token_buy_block', fromBlock);
    fromBlock += 1;
  }

  await setSyncState('last_full_refresh_at', new Date().toISOString());

  return {
    ok: true,
    scannedUntilBlock: fromBlock - 1,
    processedCount,
    results
  };
}

module.exports = {
  syncPurchasesRange
};
