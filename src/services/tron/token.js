const env = require('../../config/env');
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

async function getBuyTokenEvents({
  minBlockTimestamp,
  maxBlockTimestamp,
  fingerprint,
  limit = 100
} = {}) {
  const options = {
    eventName: 'BuyTokens',
    onlyConfirmed: true,
    orderBy: 'block_timestamp,asc',
    limit
  };

  if (typeof minBlockTimestamp === 'number') {
    options.minBlockTimestamp = minBlockTimestamp;
  }

  if (typeof maxBlockTimestamp === 'number') {
    options.maxBlockTimestamp = maxBlockTimestamp;
  }

  if (fingerprint) {
    options.fingerprint = fingerprint;
  }

  return tronWeb.getEventResult(env.FOURTEEN_TOKEN_CONTRACT, options);
}

module.exports = {
  getTransaction,
  parseBuyTransaction,
  getBuyTokenEvents
};
