require("dotenv").config();

const { BetaAnalyticsDataClient } = require("@google-analytics/data");

const propertyId = process.env.GA4_PROPERTY_ID;

if (!propertyId) {
  console.error("❌ Missing GA4_PROPERTY_ID");
  process.exit(1);
}

async function run() {
  const client = new BetaAnalyticsDataClient();

  const [response] = await client.runRealtimeReport({
    property: `properties/${propertyId}`,
    metrics: [{ name: "sessions" }],
    dimensions: [{ name: "sessionSourceMedium" }],
    minuteRanges: [
      {
        startMinutesAgo: 60,
        endMinutesAgo: 0
      }
    ]
  });

  let totalSessions = 0;
  const bySource = {};

  for (const row of response.rows || []) {
    const source = row.dimensionValues[0].value || "unknown";
    const sessions = parseInt(row.metricValues[0].value || "0", 10);

    totalSessions += sessions;
    bySource[source] = (bySource[source] || 0) + sessions;
  }

  console.log(`\nRealtime sessions (last 60 min): ${totalSessions}\n`);
  console.log("By source:");

  for (const [source, count] of Object.entries(bySource)) {
    console.log(`${source.padEnd(30)} ${count}`);
  }
}

run().catch(err => {
  console.error("❌ GA4 API error:");
  console.error(err.message || err);
});
