const { TronWeb } = require('tronweb');
const env = require('../../config/env');

const tronWeb = new TronWeb({
  fullHost: env.TRON_FULL_HOST,
  privateKey: env.TRON_PRIVATE_KEY
});

module.exports = { tronWeb };
