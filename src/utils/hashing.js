const crypto = require('crypto');

function makePurchaseId(txHash, buyerWallet) {
  return '0x' + crypto
    .createHash('sha256')
    .update(`${String(txHash).toLowerCase()}:${String(buyerWallet).toLowerCase()}`)
    .digest('hex');
}

module.exports = {
  makePurchaseId
};
