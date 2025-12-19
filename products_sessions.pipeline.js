/**
 * Product Sessions ETL Pipeline (with Backfill + Hourly Summary)
 *
 * Normal mode (hourly cron):
 *   BACKFILL_MODE=false (or unset)
 *
 * Backfill mode (runs once on startup, cron disabled):
 *   BACKFILL_MODE=true
 *   BACKFILL_START_IST_DATE=2025-10-01
 *   BACKFILL_END_IST_DATE=2025-12-14
 *
 * Optional:
 *   SHOPIFYQL_TIMEZONE=Asia/Kolkata   (default)
 */

import "dotenv/config";
import mysql from "mysql2/promise";
import axios from "axios";
import cron from "node-cron";

// ---------- Time helpers ----------
const IST_OFFSET_MIN = 330; // +05:30

function nowIST() {
  const now = new Date();
  const utc = now.getTime() + now.getTimezoneOffset() * 60000;
  return new Date(utc + IST_OFFSET_MIN * 60000);
}

function fmtIST() {
  const d = nowIST();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}T${hh}:${mi}:${ss}+05:30`;
}

function todayISTDate() {
  const d = nowIST();
  return new Date(d.getFullYear(), d.getMonth(), d.getDate());
}

function fmtDate(d) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function todayISTYMD() {
  return fmtDate(todayISTDate());
}

// ---------- Backfill env ----------
const BACKFILL_MODE = String(process.env.BACKFILL_MODE || "")
  .toLowerCase()
  .trim() === "true";
const BACKFILL_START_IST_DATE = process.env.BACKFILL_START_IST_DATE; // YYYY-MM-DD
const BACKFILL_END_IST_DATE = process.env.BACKFILL_END_IST_DATE; // YYYY-MM-DD
const SHOPIFYQL_TIMEZONE = process.env.SHOPIFYQL_TIMEZONE || "Asia/Kolkata";

// ---------- Date-range helpers (safe iteration using UTC) ----------
function isValidYMD(s) {
  return typeof s === "string" && /^\d{4}-\d{2}-\d{2}$/.test(s);
}

function ymdToUTCDate(ymd) {
  const [y, m, d] = ymd.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, d));
}

function fmtUTCDateToYMD(dt) {
  const y = dt.getUTCFullYear();
  const m = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const d = String(dt.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function addDaysUTC(dt, days) {
  return new Date(dt.getTime() + days * 86400000);
}

function buildInclusiveDateRangeYMD(startYmd, endYmd) {
  if (!isValidYMD(startYmd) || !isValidYMD(endYmd)) {
    throw new Error(
      `[BACKFILL] Invalid date format. Expected YYYY-MM-DD for BACKFILL_START_IST_DATE/BACKFILL_END_IST_DATE. Got start=${startYmd}, end=${endYmd}`
    );
  }

  const start = ymdToUTCDate(startYmd);
  const end = ymdToUTCDate(endYmd);

  if (start.getTime() > end.getTime()) {
    throw new Error(`[BACKFILL] Start date is after end date: ${startYmd} > ${endYmd}`);
  }

  const out = [];
  for (let cur = start; cur.getTime() <= end.getTime(); cur = addDaysUTC(cur, 1)) {
    out.push(fmtUTCDateToYMD(cur));
  }
  return out;
}

// ---------- Brand config ----------
function loadBrands() {
  const count = parseInt(process.env.TOTAL_CONFIG_COUNT || "0", 10);
  console.log(`[INIT] Loading ${count} brands...`);

  const brands = [];

  for (let i = 0; i < count; i++) {
    const shopName = process.env[`SHOP_NAME_${i}`];
    const apiVersion = process.env[`API_VERSION_${i}`];
    const accessToken = process.env[`ACCESS_TOKEN_${i}`];

    const dbHost = process.env[`DB_HOST_${i}`];
    const dbUser = process.env[`DB_USER_${i}`];
    const dbPassword = process.env[`DB_PASSWORD_${i}`];
    const dbDatabase = process.env[`DB_DATABASE_${i}`];

    const brandTag = process.env[`BRAND_TAG_${i}`] || `brand_${i}`;
    const brandName = process.env[`BRAND_NAME_${i}`] || brandTag.toUpperCase();

    if (!shopName || !apiVersion || !accessToken || !dbHost || !dbUser || !dbDatabase) {
      console.warn(`[INIT] Skipping brand index ${i} – missing env(s).`);
      continue;
    }

    const pool = mysql.createPool({
      host: dbHost,
      user: dbUser,
      password: dbPassword,
      database: dbDatabase,
      waitForConnections: true,
      connectionLimit: 5,
      queueLimit: 0,
    });

    brands.push({
      index: i,
      tag: brandTag,
      name: brandName,
      shopName,
      apiVersion,
      accessToken,
      dbDatabase,
      pool,
      _tablesEnsured: false,
    });

    console.log(`[INIT] Brand[${i}] ${brandName} ready (shop=${shopName}, db=${dbDatabase})`);
  }

  console.log(`[INIT] Active brands: ${brands.map((b) => `${b.index}:${b.name}`).join(", ")}`);
  return brands;
}

// Reuse pools across runs
let _BRANDS = null;
function getBrands() {
  if (!_BRANDS) _BRANDS = loadBrands();
  return _BRANDS;
}

// ---------- DB Setup ----------
async function ensureTablesForBrand(brand) {
  if (brand._tablesEnsured) return;

  const conn = await brand.pool.getConnection();
  try {
    await conn.query(`
      CREATE TABLE IF NOT EXISTS product_sessions_snapshot (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        date DATE NOT NULL,
        landing_page_type VARCHAR(100) NOT NULL,
        landing_page_path VARCHAR(500) NOT NULL,

        utm_source   VARCHAR(255) NULL,
        utm_medium   VARCHAR(255) NULL,
        utm_campaign VARCHAR(255) NULL,
        utm_content  VARCHAR(255) NULL,
        utm_term     VARCHAR(255) NULL,
        referrer_name VARCHAR(255) NULL,

        sessions INT DEFAULT 0,
        sessions_with_cart_additions INT DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        KEY idx_date (date),
        KEY idx_date_path (date, landing_page_path(200)),
        KEY idx_page_path (landing_page_path(200)),
        KEY idx_date_campaign (date, utm_campaign(100)),
        KEY idx_date_referrer (date, referrer_name(100))
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    await conn.query(`
      CREATE TABLE IF NOT EXISTS mv_product_sessions_by_type_daily (
        date DATE NOT NULL,
        landing_page_type VARCHAR(100) NOT NULL,
        sessions INT NOT NULL DEFAULT 0,
        sessions_with_cart_additions INT NOT NULL DEFAULT 0,
        add_to_cart_rate DECIMAL(6,4) NOT NULL DEFAULT 0,
        PRIMARY KEY (date, landing_page_type),
        KEY idx_type (landing_page_type)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    // Note: includes add_to_cart_rate_pct because your INSERT uses it
    await conn.query(`
      CREATE TABLE IF NOT EXISTS mv_product_sessions_by_path_daily (
        date DATE NOT NULL,
        landing_page_path VARCHAR(500) NOT NULL,
        product_id VARCHAR(50) DEFAULT NULL,
        sessions INT NOT NULL DEFAULT 0,
        sessions_with_cart_additions INT NOT NULL DEFAULT 0,
        add_to_cart_rate DECIMAL(6,4) NOT NULL DEFAULT 0,
        add_to_cart_rate_pct DECIMAL(7,4) NOT NULL DEFAULT 0,
        conversion_rate_pct DECIMAL(7,4) NOT NULL DEFAULT 0,
        PRIMARY KEY (date, landing_page_path(200)),
        KEY idx_date (date),
        KEY idx_sessions (date, sessions DESC),
        KEY idx_product_id (product_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    await conn.query(`
      CREATE TABLE IF NOT EXISTS product_landing_mapping (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        product_id BIGINT NOT NULL,
        landing_page_path VARCHAR(500) NOT NULL,
        status VARCHAR(50) DEFAULT NULL,
        title VARCHAR(255) DEFAULT NULL,
        last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_landing_page_path (landing_page_path(200)),
        KEY idx_product_id (product_id),
        KEY idx_last_synced_at (last_synced_at)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    // Note: includes landing_page_path because your INSERT uses it
    await conn.query(`
      CREATE TABLE IF NOT EXISTS mv_product_sessions_by_campaign_daily (
        date DATE NOT NULL,
        landing_page_path VARCHAR(500) NOT NULL,
        utm_campaign VARCHAR(255) NOT NULL,
        product_id   VARCHAR(50)  DEFAULT NULL,
        referrer_name VARCHAR(255) DEFAULT NULL,
        sessions INT NOT NULL DEFAULT 0,
        sessions_with_cart_additions INT NOT NULL DEFAULT 0,
        add_to_cart_rate_pct   DECIMAL(7,4) NOT NULL DEFAULT 0,
        conversion_rate_pct    DECIMAL(7,4) NOT NULL DEFAULT 0,
        PRIMARY KEY (date, utm_campaign, landing_page_path(200), product_id, referrer_name(100)),
        KEY idx_date      (date),
        KEY idx_campaign  (utm_campaign),
        KEY idx_product   (product_id),
        KEY idx_referrer  (referrer_name(100))
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    await conn.query(`
      CREATE TABLE IF NOT EXISTS pipeline_metadata (
        key_name VARCHAR(100) PRIMARY KEY,
        key_value VARCHAR(255) NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    // Hourly summary table (your requested target)
    await conn.query(`
      CREATE TABLE IF NOT EXISTS hourly_sessions_summary_shopify (
        date DATE NOT NULL,
        hour TINYINT UNSIGNED NOT NULL,
        number_of_sessions INT DEFAULT 0,
        number_of_atc_sessions INT DEFAULT 0,
        adjusted_number_of_sessions INT NULL,
        PRIMARY KEY (date, hour),
        KEY idx_date (date)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    brand._tablesEnsured = true;
  } finally {
    conn.release();
  }
}

// ---------- Metadata helpers ----------
async function getLastProductSyncDate(brand) {
  const conn = await brand.pool.getConnection();
  try {
    const [rows] = await conn.query(
      `SELECT key_value FROM pipeline_metadata WHERE key_name='last_product_sync_date'`
    );
    if (!rows.length) return null;
    return rows[0].key_value;
  } finally {
    conn.release();
  }
}

async function setLastProductSyncDate(brand, dateStr) {
  const conn = await brand.pool.getConnection();
  try {
    await conn.query(
      `
      INSERT INTO pipeline_metadata (key_name, key_value)
      VALUES ('last_product_sync_date', ?)
      ON DUPLICATE KEY UPDATE key_value = VALUES(key_value)
    `,
      [dateStr]
    );
  } finally {
    conn.release();
  }
}

// ---------- Product sync (patched pagination) ----------
async function syncProductsForBrand(brand) {
  console.log(`[${brand.tag}] Running daily product sync...`);

  const conn = await brand.pool.getConnection();
  try {
    let url = `https://${brand.shopName}.myshopify.com/admin/api/${brand.apiVersion}/products.json?limit=250&fields=id,title,status,handle`;
    let page = 1;
    let total = 0;

    while (url) {
      console.log(`[${brand.tag}] Fetching products page ${page}...`);

      const resp = await axios.get(url, {
        headers: {
          "X-Shopify-Access-Token": brand.accessToken,
          "Content-Type": "application/json",
        },
        validateStatus: () => true,
      });

      if (resp.status === 429) {
        const retry = Number(resp.headers["retry-after"] || "3");
        console.log(`[${brand.tag}] Rate-limited, sleeping ${retry}s`);
        await new Promise((r) => setTimeout(r, retry * 1000));
        continue;
      }

      if (resp.status !== 200) {
        console.error(`[${brand.tag}] Product sync failed:`, resp.status, resp.data);
        break;
      }

      const products = resp.data.products || [];
      if (!products.length) break;

      const rows = products.map((p) => [p.id, `/products/${p.handle}`, p.status || null, p.title || null]);

      if (rows.length) {
        const placeholders = rows.map(() => "(?, ?, ?, ?)").join(", ");
        await conn.query(
          `
          INSERT INTO product_landing_mapping (product_id, landing_page_path, status, title)
          VALUES ${placeholders}
          ON DUPLICATE KEY UPDATE
            product_id=VALUES(product_id),
            status=VALUES(status),
            title=VALUES(title),
            last_synced_at=CURRENT_TIMESTAMP
        `,
          rows.flat()
        );
        total += rows.length;
      }

      const link = resp.headers.link || resp.headers.Link;
      if (!link) {
        url = null;
      } else {
        const nextPart = link.split(",").find((l) => l.includes('rel="next"'));
        if (!nextPart) {
          url = null;
        } else {
          const matchUrl = nextPart.match(/<([^>]+)>/);
          url = matchUrl ? matchUrl[1] : null;
        }
      }

      page++;
    }

    console.log(`[${brand.tag}] Product sync completed. Upserted ~${total} rows.`);
  } catch (err) {
    console.error(`[${brand.tag}] Product sync failed:`, err);
  } finally {
    conn.release();
  }
}

// ---------- ShopifyQL helpers ----------
function formatShopifyQLTable(tableData) {
  const columns = tableData.columns || [];
  const rows = tableData.rows || [];
  const out = [];

  for (const row of rows) {
    // NEW STYLE (object rows)
    if (row && typeof row === "object" && !Array.isArray(row)) {
      out.push({ ...row });
      continue;
    }

    // LEGACY STYLE (array rows)
    if (Array.isArray(row)) {
      const obj = {};
      row.forEach((val, idx) => {
        if (columns[idx]) obj[columns[idx].name] = val;
      });
      out.push(obj);
    }
  }

  return out;
}

function buildDayClause(targetYmd) {
  // Avoid "future UNTIL" for today; use DURING today for current day runs.
  if (!targetYmd || targetYmd === todayISTYMD()) return `DURING today`;
  return `SINCE ${targetYmd}T00:00:00 UNTIL ${targetYmd}T23:59:59`;
}

function buildShopifyQLQuery(targetYmd = null) {
  const dayClause = buildDayClause(targetYmd);
  const tzClause = `WITH TIMEZONE '${SHOPIFYQL_TIMEZONE}'`;

  return `
    FROM sessions
      SHOW
        landing_page_type,
        landing_page_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        referrer_name,
        sessions,
        sessions_with_cart_additions
      WHERE landing_page_path IS NOT NULL
        AND human_or_bot_session IN ('human', 'bot')
      GROUP BY
        landing_page_type,
        landing_page_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        referrer_name
      ${tzClause}
      ${dayClause}
      ORDER BY sessions DESC
      LIMIT 1000
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
  `.replace(/\n+/g, " ");
}

async function fetchShopifyQLSessions(brand, targetYmd = null) {
  const url = `https://${brand.shopName}.myshopify.com/admin/api/2025-10/graphql.json`;
  const q = buildShopifyQLQuery(targetYmd).replace(/"/g, '\\"');

  const graphql = {
    query: `query { shopifyqlQuery(query: "${q}") { tableData { rows columns { name } } parseErrors } }`,
  };

  while (true) {
    const resp = await axios.post(url, graphql, {
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": brand.accessToken,
      },
      timeout: 60000,
      validateStatus: () => true,
    });

    if (resp.status === 429) {
      const retry = Number(resp.headers["retry-after"] || "3");
      console.log(`[${brand.tag}] ShopifyQL rate-limited, sleeping ${retry}s`);
      await new Promise((r) => setTimeout(r, retry * 1000));
      continue;
    }

    if (resp.status !== 200 || resp.data.errors) return [];

    const res = resp.data.data?.shopifyqlQuery;
    if (!res || res.parseErrors?.length) return [];

    return formatShopifyQLTable(res.tableData);
  }
}

// ---------- Hourly ShopifyQL (hour_of_day) ----------
function buildShopifyQLHourlyQuery(targetYmd = null) {
  const dayClause = buildDayClause(targetYmd);
  const tzClause = `WITH TIMEZONE '${SHOPIFYQL_TIMEZONE}'`;

  return `
    FROM sessions
      SHOW
        hour_of_day,
        sessions,
        sessions_with_cart_additions
      WHERE human_or_bot_session IN ('human', 'bot')
      GROUP BY hour_of_day
      ${tzClause}
      ${dayClause}
      ORDER BY hour_of_day ASC
      LIMIT 1000
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
  `.replace(/\n+/g, " ");
}

async function fetchShopifyQLHourlySessions(brand, targetYmd = null) {
  const url = `https://${brand.shopName}.myshopify.com/admin/api/2025-10/graphql.json`;
  const q = buildShopifyQLHourlyQuery(targetYmd).replace(/"/g, '\\"');

  const graphql = {
    query: `query { shopifyqlQuery(query: "${q}") { tableData { rows columns { name } } parseErrors } }`,
  };

  while (true) {
    const resp = await axios.post(url, graphql, {
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": brand.accessToken,
      },
      timeout: 60000,
      validateStatus: () => true,
    });

    if (resp.status === 429) {
      const retry = Number(resp.headers["retry-after"] || "3");
      console.log(`[${brand.tag}] ShopifyQL hourly rate-limited, sleeping ${retry}s`);
      await new Promise((r) => setTimeout(r, retry * 1000));
      continue;
    }

    if (resp.status !== 200 || resp.data.errors) return [];

    const res = resp.data.data?.shopifyqlQuery;
    if (!res || res.parseErrors?.length) return [];

    return formatShopifyQLTable(res.tableData);
  }
}

// ---------- Snapshot + MV refresh ----------
async function upsertProductSessionsSnapshot(brand, rows, targetYmd) {
  const conn = await brand.pool.getConnection();

  try {
    await conn.query(`DELETE FROM product_sessions_snapshot WHERE date=?`, [targetYmd]);
    if (!rows.length) return;

    const insertRows = rows.map((r) => [
      targetYmd,
      r.landing_page_type || null,
      r.landing_page_path || null,
      r.utm_source || null,
      r.utm_medium || null,
      r.utm_campaign || null,
      r.utm_content || null,
      r.utm_term || null,
      r.referrer_name || null,
      Number(r.sessions || 0),
      Number(r.sessions_with_cart_additions || 0),
      new Date(),
    ]);

    const placeholders = insertRows.map(() => "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").join(", ");

    await conn.query(
      `
      INSERT INTO product_sessions_snapshot
      (
        date,
        landing_page_type,
        landing_page_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        referrer_name,
        sessions,
        sessions_with_cart_additions,
        created_at
      )
      VALUES ${placeholders}
    `,
      insertRows.flat()
    );

    console.log(`[${brand.name}] Inserted ${insertRows.length} rows into snapshot for ${targetYmd}.`);
  } finally {
    conn.release();
  }
}

async function refreshMaterializedViews(brand, targetYmd) {
  const conn = await brand.pool.getConnection();

  try {
    // Wipe target day's rows from all MVs
    await conn.query(`DELETE FROM mv_product_sessions_by_type_daily     WHERE date = ?`, [targetYmd]);
    await conn.query(`DELETE FROM mv_product_sessions_by_path_daily     WHERE date = ?`, [targetYmd]);
    await conn.query(`DELETE FROM mv_product_sessions_by_campaign_daily WHERE date = ?`, [targetYmd]);

    // ---------- TYPE MV ----------
    await conn.query(
      `
      INSERT INTO mv_product_sessions_by_type_daily
      (date, landing_page_type, sessions, sessions_with_cart_additions, add_to_cart_rate)
      SELECT
        date,
        landing_page_type,
        SUM(sessions) AS sessions,
        SUM(sessions_with_cart_additions) AS sessions_with_cart_additions,
        CASE WHEN SUM(sessions) > 0
             THEN ROUND(SUM(sessions_with_cart_additions) / SUM(sessions), 4)
             ELSE 0 END AS add_to_cart_rate
      FROM product_sessions_snapshot
      WHERE date = ?
      GROUP BY date, landing_page_type
    `,
      [targetYmd]
    );

    // ---------- PATH MV ----------
    await conn.query(
      `
      INSERT INTO mv_product_sessions_by_path_daily
      (
        date,
        landing_page_path,
        product_id,
        sessions,
        sessions_with_cart_additions,
        add_to_cart_rate,
        add_to_cart_rate_pct,
        conversion_rate_pct
      )
      SELECT
        s.date,
        s.landing_page_path,
        m.product_id,
        SUM(s.sessions) AS sessions,
        SUM(s.sessions_with_cart_additions) AS sessions_with_cart_additions,

        CASE
          WHEN SUM(s.sessions) > 0
            THEN ROUND(SUM(s.sessions_with_cart_additions) / SUM(s.sessions), 4)
          ELSE 0
        END AS add_to_cart_rate,

        CASE
          WHEN SUM(s.sessions) > 0
            THEN ROUND(SUM(s.sessions_with_cart_additions) / SUM(s.sessions) * 100, 4)
          ELSE 0
        END AS add_to_cart_rate_pct,

        CASE
          WHEN SUM(s.sessions) > 0
            THEN ROUND(COALESCE(o.orders, 0) / SUM(s.sessions) * 100, 4)
          ELSE 0
        END AS conversion_rate_pct

      FROM product_sessions_snapshot s

      LEFT JOIN product_landing_mapping m
        ON s.landing_page_path = m.landing_page_path

      LEFT JOIN (
        SELECT
          product_id,
          created_dt AS created_date,
          COUNT(DISTINCT order_id) AS orders
        FROM shopify_orders
        WHERE created_dt = ?
          AND product_id IS NOT NULL
        GROUP BY product_id, created_dt
      ) o
        ON o.product_id   = m.product_id
       AND o.created_date = s.date

      WHERE s.date = ?

      GROUP BY
        s.date,
        s.landing_page_path,
        m.product_id
    `,
      [targetYmd, targetYmd]
    );

    // ---------- CAMPAIGN MV ----------
    await conn.query(
      `
      INSERT INTO mv_product_sessions_by_campaign_daily
      (
        date,
        landing_page_path,
        utm_campaign,
        product_id,
        referrer_name,
        sessions,
        sessions_with_cart_additions,
        add_to_cart_rate_pct,
        conversion_rate_pct
      )
      SELECT
        s.date,
        s.landing_page_path,
        COALESCE(s.utm_campaign, '(none)') AS utm_campaign,
        m.product_id,
        s.referrer_name,
        SUM(s.sessions) AS sessions,
        SUM(s.sessions_with_cart_additions) AS sessions_with_cart_additions,

        CASE
          WHEN SUM(s.sessions) > 0
            THEN ROUND(SUM(s.sessions_with_cart_additions) / SUM(s.sessions) * 100, 4)
          ELSE 0
        END AS add_to_cart_rate_pct,

        CASE
          WHEN SUM(s.sessions) > 0
            THEN ROUND(COALESCE(o.orders, 0) / SUM(s.sessions) * 100, 4)
          ELSE 0
        END AS conversion_rate_pct

      FROM product_sessions_snapshot s

      LEFT JOIN product_landing_mapping m
        ON s.landing_page_path = m.landing_page_path

      LEFT JOIN (
        SELECT
          product_id,
          created_dt AS created_date,
          COUNT(DISTINCT order_id) AS orders
        FROM shopify_orders
        WHERE created_dt = ?
          AND product_id IS NOT NULL
        GROUP BY product_id, created_dt
      ) o
        ON o.product_id   = m.product_id
       AND o.created_date = s.date

      WHERE
        s.date = ?
        AND s.utm_campaign IS NOT NULL

      GROUP BY
        s.date,
        s.landing_page_path,
        utm_campaign,
        m.product_id,
        s.referrer_name
    `,
      [targetYmd, targetYmd]
    );

    console.log(`[${brand.name}] Refreshed MVs for ${targetYmd}`);
  } finally {
    conn.release();
  }
}

// ---------- Hourly summary upsert ----------
async function upsertHourlySessionsSummary(brand, hourlyRows, targetYmd) {
  const conn = await brand.pool.getConnection();
  try {
    await conn.query(`DELETE FROM hourly_sessions_summary_shopify WHERE date = ?`, [targetYmd]);

    const byHour = new Map();
    for (const r of hourlyRows) {
      const h = Number(r.hour_of_day);
      if (!Number.isFinite(h) || h < 0 || h > 23) continue;
      byHour.set(h, {
        sessions: Number(r.sessions || 0),
        atc: Number(r.sessions_with_cart_additions || 0),
      });
    }

    const insertRows = [];
    for (let h = 0; h < 24; h++) {
      const v = byHour.get(h) || { sessions: 0, atc: 0 };
      insertRows.push([targetYmd, h, v.sessions, v.atc, null]);
    }

    const placeholders = insertRows.map(() => "(?, ?, ?, ?, ?)").join(", ");
    await conn.query(
      `
      INSERT INTO hourly_sessions_summary_shopify
        (date, hour, number_of_sessions, number_of_atc_sessions, adjusted_number_of_sessions)
      VALUES ${placeholders}
      `,
      insertRows.flat()
    );

    console.log(`[${brand.name}] Hourly sessions summary populated for ${targetYmd}`);
  } finally {
    conn.release();
  }
}

// ---------- Pipeline per brand (date-aware) ----------
async function processBrand(brand, targetYmd) {
  console.log(`\n========== ${brand.tag} (${targetYmd}) ==========\n`);

  await ensureTablesForBrand(brand);

  // Product sync once per real IST day (not per backfill date)
  const realToday = todayISTYMD();
  const lastSync = await getLastProductSyncDate(brand);

  if (lastSync !== realToday) {
    await syncProductsForBrand(brand);
    await setLastProductSyncDate(brand, realToday);
  } else {
    console.log(`[${brand.tag}] Product sync already done today (${realToday}), skipping.`);
  }

  // Main sessions snapshot + MVs
  const rows = await fetchShopifyQLSessions(brand, targetYmd);
  await upsertProductSessionsSnapshot(brand, rows, targetYmd);
  await refreshMaterializedViews(brand, targetYmd);

  // Hourly summary
  const hourly = await fetchShopifyQLHourlySessions(brand, targetYmd);
  await upsertHourlySessionsSummary(brand, hourly, targetYmd);

  console.log(`[${brand.tag}] Pipeline complete for ${targetYmd}.`);
}

// ---------- Runners ----------
async function runPipelineForDate(targetYmd) {
  const brands = getBrands();
  console.log(`\n🚀 Product Sessions Pipeline @ ${fmtIST()} (target=${targetYmd})\n`);
  await Promise.all(brands.map((b) => processBrand(b, targetYmd)));
  console.log(`\n✅ All brands completed for ${targetYmd}.\n`);
}

async function runBackfillPipeline() {
  if (!BACKFILL_START_IST_DATE || !BACKFILL_END_IST_DATE) {
    throw new Error(
      `[BACKFILL] BACKFILL_MODE=true requires BACKFILL_START_IST_DATE and BACKFILL_END_IST_DATE`
    );
  }

  const dates = buildInclusiveDateRangeYMD(BACKFILL_START_IST_DATE, BACKFILL_END_IST_DATE);
  const brands = getBrands();

  console.log(`\n🧱 Backfill mode enabled @ ${fmtIST()}`);
  console.log(`[BACKFILL] Range: ${dates[0]} → ${dates[dates.length - 1]} (${dates.length} days)\n`);

  for (const d of dates) {
    console.log(`\n🗓️ [BACKFILL] Running for ${d} ...\n`);
    await Promise.all(brands.map((b) => processBrand(b, d)));
  }

  console.log(`\n🏁 Backfill complete for ${dates.length} days.\n`);
}

// ---------- Scheduler wrapper ----------
let running = false;

async function safeRun(trigger = "unknown") {
  if (running) {
    console.log(`[SCHED] Skipping run (${trigger}) because previous run is still running.`);
    return;
  }

  running = true;
  const startedAt = fmtIST();
  console.log(`\n[SCHED] Starting pipeline (${trigger}) @ ${startedAt}\n`);

  try {
    if (BACKFILL_MODE) {
      await runBackfillPipeline();
    } else {
      await runPipelineForDate(todayISTYMD());
    }
    console.log(`\n[SCHED] Pipeline completed (${trigger}) @ ${fmtIST()}\n`);
  } catch (err) {
    console.error(`[SCHED] Pipeline crashed (${trigger}) @ ${fmtIST()}:`, err);
  } finally {
    running = false;
  }
}

// ---------- Main ----------
if (import.meta.url === `file://${process.argv[1]}`) {
  if (BACKFILL_MODE) {
    // Backfill once on startup; do NOT schedule hourly cron
    safeRun("backfill_startup");
    console.log(`[SCHED] Backfill mode is ON; cron is disabled.`);
    console.log(`[SCHED] Service started @ ${fmtIST()}`);
  } else {
    // 1) Run immediately on deployment/startup
    safeRun("startup");

    // 2) Cron: run at the start of each hour (minute 0, second 0) in Asia/Kolkata
    cron.schedule(
      "0 0 * * * *", // second minute hour day month weekday
      () => safeRun("hourly"),
      { timezone: "Asia/Kolkata" }
    );

    console.log(`[SCHED] Cron enabled: runs at start of every hour (Asia/Kolkata).`);
    console.log(`[SCHED] Service started @ ${fmtIST()}`);
  }
}
