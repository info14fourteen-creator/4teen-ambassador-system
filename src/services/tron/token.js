const { tronWeb } = require('./client');

async function getTransaction(txHash) {
  const tx = await tronWeb.trx.getTransaction(txHash);
  const info = await tronWeb.trx.getTransactionInfo(txHash);

  return { tx, info };
}

function parseBuyTransaction(tx, info) {
  const contract = tx?.raw_data?.contract?.[0];
  const value = contract?.parameter?.value;

  if (!value?.owner_address) {
    throw new Error('Invalid purchase transaction');
  }

  const buyerWallet = tronWeb.address.fromHex(value.owner_address);
  const purchaseAmountSun = String(value.call_value || 0);
  const ownerShareSun = String(Math.floor(Number(purchaseAmountSun) * 0.07));
  const tokenBlockNumber = info?.blockNumber || null;
  const tokenBlockTime = info?.blockTimeStamp
    ? new Date(info.blockTimeStamp).toISOString()
    : null;

  return {
    buyerWallet,
    purchaseAmountSun,
    ownerShareSun,
    tokenBlockNumber,
    tokenBlockTime
  };
}

module.exports = {
  getTransaction,
  parseBuyTransaction
};
