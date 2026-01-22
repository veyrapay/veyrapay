#!/usr/bin/env node
// poller-paypal.js (throttled + 429 backoff + realtime window cap)

const path = require('path');
require('dotenv').config({
  path: path.join(__dirname, '.env'), // always load /opt/veyrapay/api/.env
  quiet: true,
});

const { Pool } = require('pg');

if (!process.env.DATABASE_URL || typeof process.env.DATABASE_URL !== 'string') {
  throw new Error(`DATABASE_URL missing/invalid (type=${typeof process.env.DATABASE_URL})`);
}

const PAYPAL_API = process.env.PAYPAL_API || 'https://api-m.paypal.com';

// “Realtime mode” — keep it small to avoid rate limits
const MAX_WINDOW_HOURS = Number(process.env.PAYPAL_MAX_WINDOW_HOURS || 6); // cap window to last N hours
const OVERLAP_MINUTES = Number(process.env.PAYPAL_OVERLAP_MINUTES || 120);

// Request tuning
const PAGE_SIZE = Number(process.env.PAYPAL_PAGE_SIZE || 100); // smaller pages reduce payload
const REQ_TIMEOUT_MS = Number(process.env.PAYPAL_REQ_TIMEOUT_MS || 20000);
const BETWEEN_ACCOUNT_SLEEP_MS = Number(process.env.PAYPAL_BETWEEN_ACCOUNT_SLEEP_MS || 1500);
const BETWEEN_PAGE_SLEEP_MS = Number(process.env.PAYPAL_BETWEEN_PAGE_SLEEP_MS || 500);

// Backoff tuning
const MAX_429_RETRIES = Number(process.env.PAYPAL_MAX_429_RETRIES || 6);
const BASE_BACKOFF_MS = Number(process.env.PAYPAL_BASE_BACKOFF_MS || 5000);
const MAX_BACKOFF_MS = Number(process.env.PAYPAL_MAX_BACKOFF_MS || 120000);

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

// PayPal Reporting event codes
// T0006 = payment received (we treat as “capture/inflow” only if amount > 0)
const PAYPAL_EVENTS = new Set(['T0006']);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function toISO(ms) {
  return new Date(ms).toISOString();
}

function safeNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function jitter(ms) {
  // +/- 20%
  const j = ms * (0.8 + Math.random() * 0.4);
  return Math.floor(j);
}

async function fetchWithTimeout(url, options = {}, timeoutMs = REQ_TIMEOUT_MS) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal,
      headers: {
        Accept: 'application/json',
        ...(options.headers || {}),
      },
    });
  } finally {
    clearTimeout(t);
  }
}

/* ===============================
   Credential table discovery
   =============================== */

async function findCredTable() {
  const res = await pool.query(`
    SELECT
      table_schema,
      table_name,
      MAX(CASE WHEN column_name='paypal_client_id' THEN 1 ELSE 0 END) AS has_id,
      MAX(CASE WHEN column_name='paypal_client_secret' THEN 1 ELSE 0 END) AS has_secret,
      MAX(CASE WHEN column_name='account_id' THEN 1 ELSE 0 END) AS has_account,
      MAX(CASE WHEN column_name='provider' THEN 1 ELSE 0 END) AS has_provider,
      MAX(CASE WHEN column_name='provider_name' THEN 1 ELSE 0 END) AS has_provider_name
    FROM information_schema.columns
    WHERE table_schema='public'
      AND column_name IN ('paypal_client_id','paypal_client_secret','account_id','provider','provider_name')
    GROUP BY table_schema, table_name
    HAVING
      MAX(CASE WHEN column_name='paypal_client_id' THEN 1 ELSE 0 END)=1
      AND MAX(CASE WHEN column_name='paypal_client_secret' THEN 1 ELSE 0 END)=1
      AND MAX(CASE WHEN column_name='account_id' THEN 1 ELSE 0 END)=1
    ORDER BY
      (MAX(CASE WHEN column_name='provider' THEN 1 ELSE 0 END) + MAX(CASE WHEN column_name='provider_name' THEN 1 ELSE 0 END)) DESC,
      table_name ASC
    LIMIT 1;
  `);

  if (!res.rows[0]) {
    throw new Error(
      'Could not auto-discover PayPal credential table (needs paypal_client_id, paypal_client_secret, account_id).'
    );
  }

  const r = res.rows[0];
  return {
    schema: r.table_schema,
    table: r.table_name,
    hasProvider: r.has_provider === 1,
    hasProviderName: r.has_provider_name === 1,
  };
}

async function loadPaypalAccounts() {
  const cred = await findCredTable();
  const fqn = `"${cred.schema}"."${cred.table}"`;

  const whereParts = [];
  if (cred.hasProvider) whereParts.push(`c.provider = 'paypal'`);
  if (cred.hasProviderName) whereParts.push(`c.provider_name = 'paypal'`);
  const whereClause = whereParts.length ? `WHERE (${whereParts.join(' OR ')})` : '';

  const q = `
    SELECT
      a.id AS account_id,
      a.label AS label,
      c.paypal_client_id,
      c.paypal_client_secret
    FROM ${fqn} c
    JOIN accounts a ON a.id = c.account_id
    ${whereClause}
    ORDER BY a.label ASC;
  `;

  const res = await pool.query(q);
  return res.rows;
}

/* ===============================
   PayPal API
   =============================== */

async function getAccessToken(clientId, clientSecret) {
  const auth = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');

  const resp = await fetchWithTimeout(`${PAYPAL_API}/v1/oauth2/token`, {
    method: 'POST',
    headers: {
      Authorization: `Basic ${auth}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: 'grant_type=client_credentials',
  });

  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`OAuth failed (${resp.status}) ${text}`.slice(0, 350));
  }

  const json = await resp.json();
  if (!json?.access_token) throw new Error('OAuth response missing access_token');
  return json.access_token;
}

function parseRetryAfterMs(resp) {
  const ra = resp.headers?.get?.('retry-after');
  if (!ra) return null;
  const sec = Number(ra);
  if (Number.isFinite(sec) && sec > 0) return sec * 1000;
  return null;
}

async function fetchReportingPage(accessToken, startISO, endISO, page) {
  const url =
    `${PAYPAL_API}/v1/reporting/transactions` +
    `?start_date=${encodeURIComponent(startISO)}` +
    `&end_date=${encodeURIComponent(endISO)}` +
    `&fields=transaction_info` +
    `&page_size=${PAGE_SIZE}` +
    `&page=${page}`;

  return await fetchWithTimeout(url, {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
}

async function fetchTransactions(accessToken, startISO, endISO) {
  let page = 1;
  const all = [];

  while (true) {
    console.log(`   📡 reporting page ${page} (timeout ${REQ_TIMEOUT_MS}ms)`);

    let resp;
    let attempt = 0;

    while (true) {
      attempt += 1;
      try {
        resp = await fetchReportingPage(accessToken, startISO, endISO, page);
      } catch (e) {
        // network hang/abort -> backoff + retry
        const wait = Math.min(MAX_BACKOFF_MS, jitter(BASE_BACKOFF_MS * Math.pow(2, attempt - 1)));
        if (attempt >= 3) throw new Error(`Reporting API network timeout/hang (page ${page})`);
        console.log(`   ⏳ network issue, retry in ${Math.round(wait / 1000)}s...`);
        await sleep(wait);
        continue;
      }

      if (resp.status === 429) {
        const retryAfter = parseRetryAfterMs(resp);
        const wait =
          retryAfter ??
          Math.min(MAX_BACKOFF_MS, jitter(BASE_BACKOFF_MS * Math.pow(2, attempt - 1)));

        const body = await resp.text().catch(() => '');
        console.log(
          `   🛑 429 rate limited (page ${page}) — wait ${Math.round(wait / 1000)}s` +
            (retryAfter ? ' (Retry-After)' : '')
        );
        if (body) console.log(`   ↳ ${body.slice(0, 200)}`);

        if (attempt >= MAX_429_RETRIES) {
          throw new Error('RATE_LIMIT_REACHED (too many 429 retries)');
        }

        await sleep(wait);
        continue;
      }

      // OK or other status -> break and handle below
      break;
    }

    if (!resp.ok) {
      const text = await resp.text().catch(() => '');
      throw new Error(`Reporting API failed (${resp.status}) ${text}`.slice(0, 350));
    }

    const json = await resp.json();
    const details = Array.isArray(json?.transaction_details) ? json.transaction_details : [];
    all.push(...details);

    const totalPages = Number(json?.total_pages || 1);
    console.log(`   ✅ page ${page}/${totalPages}  +${details.length}  total=${all.length}`);

    if (page >= totalPages) break;
    page += 1;

    await sleep(BETWEEN_PAGE_SLEEP_MS);
  }

  return all;
}

/* ===============================
   Cursor window (realtime cap)
   =============================== */

async function cursorWindowForAccount(accountId) {
  const res = await pool.query(
    `
    SELECT MAX(occurred_at) AS last
    FROM transactions
    WHERE provider = 'paypal'
      AND account_id = $1
    `,
    [accountId]
  );

  const now = Date.now();
  const lastMs = res.rows[0]?.last ? new Date(res.rows[0].last).getTime() : null;

  const overlapMs = OVERLAP_MINUTES * 60 * 1000;
  const capStartMs = now - MAX_WINDOW_HOURS * 60 * 60 * 1000;

  // Start from last seen - overlap, but never earlier than capStart
  const startMs = Math.max(capStartMs, (lastMs ?? capStartMs) - overlapMs);

  return { startISO: toISO(startMs), endISO: toISO(now) };
}

/* ===============================
   DB insert
   =============================== */

async function insertTransaction(m, id, eventType, payload, occurredAt) {
  const res = await pool.query(
    `
    INSERT INTO transactions (
      account_id,
      provider,
      provider_event_id,
      event_type,
      payload_json,
      occurred_at,
      verified
    )
    VALUES ($1,'paypal',$2,$3,$4,$5,true)
    ON CONFLICT (provider, provider_event_id) DO NOTHING
    RETURNING 1;
    `,
    [m.account_id, id, eventType, payload, occurredAt]
  );
  return res.rowCount || 0;
}

/* ===============================
   MAIN
   =============================== */

async function run() {
  console.log('🚀 Starting PayPal poller (throttled + backoff)');

  const merchants = await loadPaypalAccounts();
  console.log(`🔑 Found ${merchants.length} PayPal accounts\n`);

  for (const m of merchants) {
    console.log(`📦 ${m.label}`);

    try {
      const token = await getAccessToken(m.paypal_client_id, m.paypal_client_secret);
      console.log('   🔐 token ok, calling reporting API…');

      const { startISO, endISO } = await cursorWindowForAccount(m.account_id);
      console.log(`   🕒 window ${startISO} → ${endISO}`);

      const txs = await fetchTransactions(token, startISO, endISO);

      let captureCount = 0;
      let captureTotal = 0;
      let nonCaptureCount = 0;
      let inserted = 0;

      for (const t of txs) {
        const info = t?.transaction_info;
        if (!info) continue;

        const eventType = info.transaction_event_code;
        if (!PAYPAL_EVENTS.has(eventType)) continue;

        const id = info.transaction_id;
        const occurredAt =
          info.transaction_initiation_date ||
          info.transaction_updated_date ||
          info.transaction_event_date;

        if (!id || !occurredAt) continue;

        let amount = null;
        if (info.transaction_amount?.value != null) {
          amount = safeNum(info.transaction_amount.value);
        }

        // Capture should mean actual inflow only
        if (eventType === 'T0006' && amount !== null && amount > 0) {
          captureCount += 1;
          captureTotal += amount;
        } else {
          nonCaptureCount += 1;
        }

        inserted += await insertTransaction(m, id, eventType, t, occurredAt);
      }

      const skipped = (captureCount + nonCaptureCount) - inserted;

      if (captureCount > 0 || nonCaptureCount > 0) {
        console.log(
          `   ✅ ${captureCount} capture tx | TOTAL ${captureTotal.toFixed(
            2
          )} | ${nonCaptureCount} non-capture events | INSERTED ${inserted} | SKIPPED ${skipped}\n`
        );
      } else {
        console.log('   ⚠️ No transactions\n');
      }
    } catch (err) {
      const msg = String(err?.message || err);

      if (msg.includes('invalid_client')) {
        console.log(`   ❌ OAuth failed (invalid_client) — bad PayPal creds for this account\n`);
      } else if (msg.includes('NOT_AUTHORIZED') || msg.includes('insufficient permissions')) {
        console.log(`   ❌ Reporting API 403 — PayPal app lacks reporting permissions/scopes\n`);
      } else if (msg.includes('RATE_LIMIT_REACHED') || msg.includes('429')) {
        console.log(`   🛑 PayPal rate limited — poll slowed/backed off (try again shortly)\n`);
      } else {
        console.log('   ❌ PayPal poll failed');
        console.error('     ', msg, '\n');
      }
    }

    // throttle between accounts to avoid 429 across multiple merchants
    await sleep(BETWEEN_ACCOUNT_SLEEP_MS);
  }

  console.log('🏁 PayPal poll complete');
  process.exit(0);
}

run().catch((err) => {
  console.error('❌ Poller crashed', err);
  process.exit(1);
});
