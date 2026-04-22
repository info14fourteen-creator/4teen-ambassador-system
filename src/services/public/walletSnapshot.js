const env = require('../../config/env');

const TRONSCAN_BASE_URL = 'https://apilist.tronscanapi.com/api';

function buildUrl(base, path, params) {
  const url = new URL(path, base);

  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      if (value === undefined || value === null || value === '') return;
      url.searchParams.set(key, String(value));
    });
  }

  return url.toString();
}

async function safeJson(response) {
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`HTTP ${response.status}: ${text}`);
  }

  return response.json();
}

function buildHeaders(apiKey) {
  const headers = {
    Accept: 'application/json'
  };

  if (apiKey) {
    headers['TRON-PRO-API-KEY'] = apiKey;
  }

  return headers;
}

function formatTokenBalance(rawBalance, decimals) {
  const balance = typeof rawBalance === 'number' ? String(rawBalance) : String(rawBalance || '0');
  const value = Number(balance) / Math.pow(10, decimals);

  if (!Number.isFinite(value)) return '0';

  return value.toLocaleString('en-US', {
    minimumFractionDigits: 0,
    maximumFractionDigits: Math.min(decimals, 6)
  });
}

async function fetchTrongrid(path, params) {
  const url = buildUrl(env.TRON_FULL_HOST, path, params);
  const response = await fetch(url, {
    headers: buildHeaders(env.TRONGRID_API_KEY)
  });

  return safeJson(response);
}

async function fetchTronscan(path, params) {
  const apiKey = env.TRONSCAN_API_KEY || env.TRONGRID_API_KEY;
  const url = buildUrl(TRONSCAN_BASE_URL, path, params);
  const response = await fetch(url, {
    headers: buildHeaders(apiKey)
  });

  return safeJson(response);
}

async function getAccountInfo(address) {
  const data = await fetchTrongrid(`/v1/accounts/${address}`);
  const item = Array.isArray(data?.data) ? data.data[0] : null;
  const balanceSun = Number(item?.balance || 0);

  return {
    address,
    balanceSun,
    balanceTrx: balanceSun / 1_000_000
  };
}

async function getAccountTrc20Assets(address) {
  const data = await fetchTronscan('/account/tokens', { address });
  const balances = Array.isArray(data?.trc20token_balances) ? data.trc20token_balances : [];

  return balances
    .filter((item) => item?.tokenId && item?.balance && Number(item.balance) > 0)
    .map((item) => {
      const decimals = Number(item.tokenDecimal ?? 0);
      const balance = String(item.balance ?? '0');

      const priceInUsdRaw =
        item.priceInUsd ??
        item.tokenPriceInUsd;

      const valueInUsdRaw =
        item.amountInUsd ??
        item.balanceInUsd;

      const priceChangeRaw =
        item.price_change_24h ??
        item.priceChange24h;

      return {
        tokenId: item.tokenId ?? '',
        tokenName: item.tokenName ?? '',
        tokenAbbr: item.tokenAbbr ?? '',
        tokenLogo: item.tokenLogo,
        balance,
        tokenDecimal: decimals,
        balanceFormatted: formatTokenBalance(balance, decimals),
        priceInUsd:
          priceInUsdRaw === undefined || priceInUsdRaw === null
            ? undefined
            : Number(priceInUsdRaw),
        valueInUsd:
          valueInUsdRaw === undefined || valueInUsdRaw === null
            ? undefined
            : Number(valueInUsdRaw),
        priceChange24h:
          priceChangeRaw === undefined || priceChangeRaw === null
            ? undefined
            : Number(priceChangeRaw)
      };
    });
}

async function getTrxPriceInfo() {
  const data = await fetchTronscan('/token/price', {
    contract: 'T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb'
  });

  const rawPrice =
    data?.priceInUsd ??
    data?.price_in_usd ??
    0;

  const rawChange =
    data?.price_change_24h ??
    data?.change1d;

  return {
    priceInUsd: Number(rawPrice ?? 0),
    priceChange24h:
      rawChange === undefined || rawChange === null ? undefined : Number(rawChange)
  };
}

async function getWalletSnapshot(address) {
  const [account, trc20Assets, trxPrice] = await Promise.all([
    getAccountInfo(address),
    getAccountTrc20Assets(address),
    getTrxPriceInfo()
  ]);

  const trxValueInUsd = account.balanceTrx * trxPrice.priceInUsd;

  return {
    address,
    trx: {
      balanceTrx: account.balanceTrx,
      valueInUsd: trxValueInUsd,
      priceInUsd: trxPrice.priceInUsd,
      priceChange24h: trxPrice.priceChange24h
    },
    trc20Assets
  };
}

module.exports = {
  getWalletSnapshot,
  getTrxPriceInfo
};
