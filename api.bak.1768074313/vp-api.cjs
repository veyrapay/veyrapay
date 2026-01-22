const express = require("express");
const crypto = require("crypto");
const { Pool } = require("pg");
require("dotenv").config();

const PORT = 8080;
const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL env var");
  process.exit(1);
}

const pool = new Pool({ connectionString: DATABASE_URL });

const app = express();
app.set("trust proxy", true);
app.use(express.json({ limit: "64kb" }));

app.get("/vp/health", async (req, res) => {
  try {
    await pool.query("select 1");
    res.json({ ok: true, service: "vp-api" });
  } catch (e) {
    res.status(500).json({ ok: false, service: "vp-api" });
  }
});

const hits = new Map();
function rateLimit(req, res, next) {
  const ip = req.headers["cf-connecting-ip"] || req.ip || "unknown";
  const now = Date.now();
  const windowMs = 60_000;
  const max = 180;
  const cur = hits.get(ip);
  if (!cur || now > cur.resetAt) {
    hits.set(ip, { count: 1, resetAt: now + windowMs });
    return next();
  }
  cur.count++;
  if (cur.count > max) return res.status(429).end();
  next();
}

app.post("/vp/collect", rateLimit, async (req, res) => {
  const b = req.body || {};
  const anon_id = typeof b.anon_id === "string" ? b.anon_id.trim() : "";
  const occurred_at = b.occurred_at ? new Date(b.occurred_at) : new Date();
  const url = typeof b.url === "string" ? b.url : "";
  const referrer = typeof b.referrer === "string" ? b.referrer : null;

  if (!anon_id || anon_id.length > 80) return res.status(400).end();
  if (!url || url.length > 2000) return res.status(400).end();
  if (Number.isNaN(occurred_at.getTime())) return res.status(400).end();

  const click_ids = (b.click_ids && typeof b.click_ids === "object") ? b.click_ids : {};
  const utm_source = typeof b.utm_source === "string" ? b.utm_source : null;
  const utm_medium = typeof b.utm_medium === "string" ? b.utm_medium : null;
  const utm_campaign = typeof b.utm_campaign === "string" ? b.utm_campaign : null;
  const utm_content = typeof b.utm_content === "string" ? b.utm_content : null;
  const utm_term = typeof b.utm_term === "string" ? b.utm_term : null;

  const ip = req.headers["cf-connecting-ip"] || req.ip || null;
  const ua = req.headers["user-agent"] || null;

  const client = await pool.connect();
  try {
    await client.query("begin");
    await client.query(
      `insert into vp_touches
        (anon_id, occurred_at, url, referrer, utm_source, utm_medium, utm_campaign, utm_content, utm_term, click_ids, ip, user_agent)
       values
        ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
      [anon_id, occurred_at.toISOString(), url, referrer, utm_source, utm_medium, utm_campaign, utm_content, utm_term, click_ids, ip, ua]
    );

    await client.query(
      `insert into vp_identities (anon_id, first_seen_at, last_seen_at)
       values ($1, now(), now())
       on conflict (anon_id) do update set last_seen_at = now()`,
      [anon_id]
    );

    await client.query("commit");
    return res.status(204).end();
  } catch (e) {
    await client.query("rollback");
    return res.status(500).end();
  } finally {
    client.release();
  }
});

app.post("/vp/identify", rateLimit, async (req, res) => {
  const b = req.body || {};
  const anon_id = typeof b.anon_id === "string" ? b.anon_id.trim() : "";
  const email = typeof b.email === "string" ? b.email.trim().toLowerCase() : "";

  if (!anon_id || anon_id.length > 80) return res.status(400).end();
  if (!email || email.length > 254) return res.status(400).end();

  const email_hash = crypto.createHash("sha256").update(email).digest("hex");

  await pool.query(
    `insert into vp_identities (anon_id, email_hash, first_seen_at, last_seen_at)
     values ($1, $2, now(), now())
     on conflict (anon_id) do update
       set email_hash = excluded.email_hash,
           last_seen_at = now()`,
    [anon_id, email_hash]
  );

  return res.status(204).end();
});

app.listen(PORT, "127.0.0.1", () => {
  console.log(`vp-api listening on 127.0.0.1:${PORT}`);
});
