const { tronWeb } = require('./client');

function zeroAddressToNull(address) {
  if (!address) return null;

  try {
    const base58 = tronWeb.address.fromHex(address);
    if (base58 === 'T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb') {
      return null;
    }
    return base58;
  } catch (_) {
    return address;
  }
}

function normalizeAddress(address) {
  if (!address) return null;

  try {
    if (address.startsWith('T')) {
      return address;
    }

    return tronWeb.address.fromHex(address);
  } catch (_) {
    return address;
  }
}

module.exports = {
  zeroAddressToNull,
  normalizeAddress
};
