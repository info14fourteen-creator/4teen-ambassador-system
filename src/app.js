const express = require('express');
const healthRouter = require('./routes/health');
const adminRouter = require('./routes/admin');
const cabinetRouter = require('./routes/cabinet');
const env = require('./config/env');

const app = express();

function normalizeOrigin(value) {
  return String(value || '')
    .trim()
    .replace(/\/+$/, '')
    .toLowerCase();
}

function parseAllowedOrigins(value) {
  const envOrigins = String(value || '')
    .split(',')
    .map((item) => normalizeOrigin(item))
    .filter(Boolean);

  const fallbackOrigins = [
    'https://4teen.me',
    'https://www.4teen.me',
    'http://localhost:3000',
    'http://127.0.0.1:3000'
  ].map(normalizeOrigin);

  return Array.from(new Set([...fallbackOrigins, ...envOrigins]));
}

const allowedOrigins = parseAllowedOrigins(env.ALLOWED_ORIGINS);

app.disable('x-powered-by');

app.use((req, res, next) => {
  const requestOriginRaw = req.headers.origin;
  const requestOrigin = normalizeOrigin(requestOriginRaw);

  if (requestOrigin && allowedOrigins.includes(requestOrigin)) {
    res.setHeader('Access-Control-Allow-Origin', requestOriginRaw);
    res.setHeader('Vary', 'Origin');
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
    res.setHeader(
      'Access-Control-Allow-Headers',
      'Content-Type,x-admin-sync-token,authorization'
    );
    res.setHeader('Access-Control-Max-Age', '86400');
  }

  if (req.method === 'OPTIONS') {
    return res.status(204).end();
  }

  return next();
});

app.use(express.json());

app.use('/', healthRouter);
app.use('/admin', adminRouter);
app.use('/cabinet', cabinetRouter);

module.exports = app;
