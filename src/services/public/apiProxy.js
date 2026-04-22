const env = require('../../config/env');

const TRONSCAN_BASE_URL = 'https://apilist.tronscanapi.com/api';
const CMC_PRO_BASE_URL = 'https://pro-api.coinmarketcap.com';
const CMC_DATA_BASE_URL = 'https://api.coinmarketcap.com';
const CMC_DAPI_BASE_URL = 'https://dapi.coinmarketcap.com';
const KEY_COOLDOWN_MS = 60_000;
const REQUEST_TIMEOUT_MS = 15_000;

const keyPools = {
  trongrid: buildKeyPool([
    env.TRONGRID_API_KEY_1,
    env.TRONGRID_API_KEY_2,
    env.TRONGRID_API_KEY_3,
    env.TRONGRID_API_KEY
  ]),
  tronscan: buildKeyPool([
    env.TRONSCAN_API_KEY_1,
    env.TRONSCAN_API_KEY_2,
    env.TRONSCAN_API_KEY_3,
    env.TRONSCAN_API_KEY,
    env.TRONGRID_API_KEY_1,
    env.TRONGRID_API_KEY
  ]),
  cmc: buildKeyPool([
    env.CMC_API_KEY_1,
    env.CMC_API_KEY_2,
    env.CMC_API_KEY_3,
    env.CMC_API_KEY
  ])
};

const keyState = {
  trongrid: createKeyState(keyPools.trongrid.length),
  tronscan: createKeyState(keyPools.tronscan.length),
  cmc: createKeyState(keyPools.cmc.length)
};

function buildKeyPool(values) {
  return Array.from(
    new Set(
      values
        .map((value) => String(value || '').trim())
        .filter(Boolean)
    )
  );
}

function createKeyState(size) {
  return {
    nextIndex: 0,
    cooldownUntil: Array.from({ length: size }, () => 0)
  };
}

function getTargetBase(provider) {
  if (provider === 'trongrid') return env.TRON_FULL_HOST;
  if (provider === 'tronscan') return TRONSCAN_BASE_URL;
  if (provider === 'cmc-pro') return CMC_PRO_BASE_URL;
  if (provider === 'cmc-data') return CMC_DATA_BASE_URL;
  if (provider === 'cmc-dapi') return CMC_DAPI_BASE_URL;
  throw new Error(`Unsupported proxy provider: ${provider}`);
}

function getPoolName(provider) {
  if (provider === 'trongrid') return 'trongrid';
  if (provider === 'tronscan') return 'tronscan';
  if (provider.startsWith('cmc-')) return 'cmc';
  throw new Error(`Unsupported proxy provider: ${provider}`);
}

function buildTargetUrl(provider, path, query) {
  const normalizedPath = String(path || '').startsWith('/') ? path : `/${path || ''}`;
  const url = new URL(`${getTargetBase(provider).replace(/\/+$/, '')}${normalizedPath}`);

  Object.entries(query || {}).forEach(([key, value]) => {
    if (value === undefined || value === null || value === '') return;

    if (Array.isArray(value)) {
      value.forEach((item) => url.searchParams.append(key, String(item)));
      return;
    }

    url.searchParams.set(key, String(value));
  });

  return url.toString();
}

function isRateLimit(status, body) {
  const lower = String(body || '').toLowerCase();

  return (
    status === 429 ||
    (status === 403 &&
      (lower.includes('too many requests') ||
        lower.includes('rate limit') ||
        lower.includes('quota') ||
        lower.includes('limit exceeded')))
  );
}

function buildHeaders(provider, apiKey) {
  const headers = {
    Accept: 'application/json'
  };

  if (provider === 'cmc-pro' && apiKey) {
    headers['X-CMC_PRO_API_KEY'] = apiKey;
  } else if ((provider === 'trongrid' || provider === 'tronscan') && apiKey) {
    headers['TRON-PRO-API-KEY'] = apiKey;
  }

  return headers;
}

function normalizeBody(req) {
  if (req.method === 'GET' || req.method === 'HEAD') {
    return undefined;
  }

  return JSON.stringify(req.body ?? {});
}

async function proxyRequest({ provider, path, query, method = 'GET', body }) {
  const poolName = getPoolName(provider);
  const keys = keyPools[poolName];
  const state = keyState[poolName];
  const useKeyPool = provider === 'cmc-data' || provider === 'cmc-dapi' ? false : keys.length > 0;

  if (!useKeyPool) {
    return requestOnce({
      provider,
      path,
      query,
      method,
      body,
      apiKey: ''
    });
  }

  if (state.cooldownUntil.length !== keys.length) {
    state.cooldownUntil = Array.from({ length: keys.length }, () => 0);
  }

  const now = Date.now();
  const indexes = Array.from({ length: keys.length }, (_, offset) => {
    return (state.nextIndex + offset) % keys.length;
  }).filter((index) => state.cooldownUntil[index] <= now);

  if (!indexes.length) {
    const nextAvailableAt = state.cooldownUntil
      .filter((value) => value > now)
      .sort((a, b) => a - b)[0];
    const retrySeconds = Math.ceil(Math.max(0, (nextAvailableAt || now) - now) / 1000);
    const error = new Error(`All ${poolName} keys are cooling down. Retry after ${retrySeconds}s.`);
    error.status = 429;
    throw error;
  }

  let lastError = null;

  for (const index of indexes) {
    try {
      const result = await requestOnce({
        provider,
        path,
        query,
        method,
        body,
        apiKey: keys[index]
      });

      state.nextIndex = (index + 1) % keys.length;
      state.cooldownUntil[index] = 0;
      return result;
    } catch (error) {
      lastError = error;

      if (isRateLimit(error.status, error.body)) {
        state.cooldownUntil[index] = Date.now() + KEY_COOLDOWN_MS;
        continue;
      }

      if (error.status >= 400 && error.status < 500) {
        throw error;
      }
    }
  }

  throw lastError || new Error(`${provider} proxy request failed`);
}

async function requestOnce({ provider, path, query, method, body, apiKey }) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  const url = buildTargetUrl(provider, path, query);

  try {
    const response = await fetch(url, {
      method,
      headers: {
        ...buildHeaders(provider, apiKey),
        ...(method === 'GET' ? {} : { 'Content-Type': 'application/json' })
      },
      body,
      signal: controller.signal
    });
    const text = await response.text();

    if (!response.ok) {
      const error = new Error(`HTTP ${response.status}`);
      error.status = response.status;
      error.body = text;
      throw error;
    }

    return {
      status: response.status,
      contentType: response.headers.get('content-type') || 'application/json',
      body: text
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function handleProxyRequest(req, res, provider, path) {
  try {
    const result = await proxyRequest({
      provider,
      path,
      query: req.query,
      method: req.method,
      body: normalizeBody(req)
    });

    res.status(result.status);
    res.setHeader('Cache-Control', 'private, max-age=15');
    res.setHeader('Content-Type', result.contentType);
    return res.send(result.body);
  } catch (error) {
    return res.status(error.status || 502).json({
      ok: false,
      error: error.message,
      upstream: error.body ? String(error.body).slice(0, 500) : undefined
    });
  }
}

module.exports = {
  handleProxyRequest
};
