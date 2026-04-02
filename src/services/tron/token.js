const env = require('../../config/env');
const { tronWeb } = require('./client');

function toBase58Address(value) {
  if (!value) return null;

  try {
    if (typeof value === 'string' && value.startsWith('T')) {
      return value;
    }

    let hex = String(value).toLowerCase();

    if (hex.startsWith('0x')) {
      hex = hex.slice(2);
    }

    if (!hex.startsWith('41')) {
      hex = `41${hex}`;
    }

    return tronWeb.address.fromHex(hex);
  } catch (_) {
    return null;
  }
}

function normalizeEventList(response) {
  if (Array.isArray(response)) {
    return response;
  }

  if (Array.isArray(response?.data)) {
    return response.data;
  }

  return [];
}

async function getBuyTokenEvents({
  minBlockTimestamp,
  maxBlockTimestamp,
  fingerprint,
  limit = 20
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

async function getBuyEventByTxHash(txHash) {
  const response = await tronWeb.getEventByTransactionID(txHash);
  const list = normalizeEventList(response);

  const match = list.find((item) => {
    const eventName = String(item?.event_name || '');
    const contractAddress = toBase58Address(item?.contract_address);

    return eventName === 'BuyTokens' && contractAddress === env.FOURTEEN_TOKEN_CONTRACT;
  });

  if (!match) {
    throw new Error('BuyTokens event not found for transaction');
  }

  const buyerWallet = toBase58Address(match?.result?.buyer || match?.result?.['0']);
  const purchaseAmountSun = String(match?.result?.amountTRX || match?.result?.['1'] || 0);
  const tokenAmountRaw = String(match?.result?.amountTokens || match?.result?.['2'] || 0);
  const tokenBlockNumber = Number(match?.block_number || 0);
  const eventTs = Number(match?.block_timestamp || 0);

  if (!buyerWallet) {
    throw new Error('Buyer address was not found in BuyTokens event');
  }

  return {
    txHash: String(txHash).toLowerCase(),
    buyerWallet,
    purchaseAmountSun,
    ownerShareSun: String(Math.floor(Number(purchaseAmountSun) * 0.07)),
    tokenAmountRaw,
    tokenBlockNumber,
    tokenBlockTime: eventTs ? new Date(eventTs).toISOString() : new Date().toISOString(),
    blockTimestamp: eventTs
  };
}

module.exports = {
  getBuyTokenEvents,
  getBuyEventByTxHash
};
