const express = require('express');
const healthRouter = require('./routes/health');
const adminRouter = require('./routes/admin');
const cabinetRouter = require('./routes/cabinet');
const env = require('./config/env');

const app = express();

function parseAllowedOrigins(value) {
  return String(value || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

const allowedOrigins = parseAllowedOrigins(env.ALLOWED_ORIGINS);

app.use((req, res, next) => {
  const origin = req.headers.origin;

  if (origin && allowedOrigins.includes(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type,x-admin-sync-token');
  }

  if (req.method === 'OPTIONS') {
    return res.status(204).end();
  }

  next();
});

app.use(express.json());

app.use('/', healthRouter);
app.use('/admin', adminRouter);
app.use('/cabinet', cabinetRouter);

module.exports = app;
