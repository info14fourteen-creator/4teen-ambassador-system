const { Pool } = require('pg');
const env = require('../config/env');

const pool = new Pool({
  connectionString: env.DATABASE_URL,
  ssl: env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

module.exports = { pool };
