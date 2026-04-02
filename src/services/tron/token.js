const env = require('../../config/env');
const { tronWeb } = require('./client');

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

module.exports = {
  getBuyTokenEvents
};
