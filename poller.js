// poller.js — Stripe polling worker

require('dotenv').config();

const { Pool } = require('pg');
const Stripe = require('stripe');

// 1️⃣ Create Postgres pool (THIS WAS MISSING)
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// 2️⃣ Main poller
async function runPoller() {
  console.log('🚀 Starting Stripe poller...');

  // 3️⃣ Load active Stripe credentials
  const { rows } = await pool.query(`
    SELECT
      c.id AS credential_id,
      a.id AS account_id,
      a.label,
      c.stripe_secret_key
    FROM credentials c
    JOIN accounts a ON a.id = c.account_id
    WHERE c.provider = 'stripe'
      AND c.is_active = true
      AND c.stripe_secret_key IS NOT NULL
  `);

  console.log(`Found ${rows.length} Stripe keys`);

  // 4️⃣ Loop accounts
  for (const row of rows) {
    console.log(`\n📦 ${row.label}`);

    try {
      const stripe = new Stripe(row.stripe_secret_key);

      const charges = await stripe.charges.list({ limit: 10 });

      console.log(`  ${charges.data.length} charges fetched`);

      for (const ch of charges.data) {
        await pool.query(`
          INSERT INTO transactions (
            account_id,
            provider,
            provider_event_id,
            event_type,
            payload_json,
            occurred_at
          ) VALUES (
            $1,
            'stripe',
            $2,
            'charge',
            $3,
            to_timestamp($4)
          )
          ON CONFLICT (provider, provider_event_id) DO NOTHING
        `, [
          row.account_id,
          ch.id,
          ch,
          ch.created,
        ]);
      }

    } catch (err) {
      console.error(`❌ ${row.label}: ${err.message}`);
    }
  }

  console.log('\n✅ Stripe poll complete');
}

// 5️⃣ Run safely
runPoller()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('❌ Poller crashed:', err);
    process.exit(1);
  });
