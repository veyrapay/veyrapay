// live-feed.js
// Show transactions from the last 7 days

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function fetchRows() {
  const sql = `
    SELECT
      t.occurred_at,
      t.provider,
      a.label AS store,
      COALESCE(
        t.payload_json->>'customer_name',
        t.payload_json->>'billing_name',
        t.payload_json->>'name'
      ) AS buyer,
      CASE
        WHEN t.provider = 'stripe'
          THEN (t.payload_json->>'amount')::numeric / 100
        ELSE
          (t.payload_json->>'amount')::numeric
      END AS amount
    FROM transactions t
    JOIN accounts a ON a.id = t.account_id
    WHERE t.occurred_at >= now() - interval '7 days'
    ORDER BY t.occurred_at DESC;
  `;

  const { rows } = await pool.query(sql);
  return rows;
}

function header() {
  console.log('');
  console.log('transactions (last 7 days)');
  console.log('--------------------------------------------------------------------------------');
  console.log(
    'timestamp               | provider | buyer               | store                     | amount'
  );
  console.log(
    '--------------------------------------------------------------------------------'
  );
}

function formatRow(r) {
  const ts = new Date(r.occurred_at)
    .toLocaleString('en-AU', { timeZone: 'Australia/Melbourne' })
    .padEnd(23);

  const provider = r.provider.toUpperCase().padEnd(8);
  const buyer = (r.buyer || '-').padEnd(19);
  const store = r.store.padEnd(25);
  const amount = `$${Number(r.amount).toFixed(2)}`.padStart(8);

  return `${ts} | ${provider} | ${buyer} | ${store} | ${amount}`;
}

async function run() {
  header();

  const rows = await fetchRows();

  if (!rows.length) {
    console.log('no rows');
    return;
  }

  for (const r of rows) {
    console.log(formatRow(r));
  }
}

run()
  .catch(err => {
    console.error(err.message);
    process.exit(1);
  })
  .finally(() => pool.end());
