#!/usr/bin/env node
/**
 * Minimal backfill for hourly_product_sessions and hourly_product_performance_rollup.
 *
 * Usage:
 *   node scripts/backfill-hourly-product-performance.js
 *
 * Required env:
 *   BACKFILL_START_IST_DATE=YYYY-MM-DD
 *   BACKFILL_END_IST_DATE=YYYY-MM-DD
 *
 * Brand/env loading:
 *   Same API env as product_sessions_pipeline.js:
 *   GET_BRANDS_API, PIPELINE_AUTH_HEADER, PASSWORD_AES_KEY
 */

import "dotenv/config";
import axios from "axios";
import { loadPipelineBrandsForScripts } from "./brandLoader.js";

const SHOPIFYQL_TIMEZONE = process.env.SHOPIFYQL_TIMEZONE || "Asia/Kolkata";
const BACKFILL_START_IST_DATE = (
  process.env.BACKFILL_START_IST_DATE ||
  process.env.BACKFILL_START_IST ||
  ""
).split("T")[0];
const BACKFILL_END_IST_DATE = (
  process.env.BACKFILL_END_IST_DATE ||
  process.env.BACKFILL_END_IST ||
  ""
).split("T")[0];
const BACKFILL_BRAND_CONCURRENCY = Math.max(
  1,
  parseInt(process.env.BACKFILL_BRAND_CONCURRENCY || "6", 10) || 6,
);
const SHOPIFYQL_PAGE_SIZE = Math.max(
  1,
  parseInt(process.env.SHOPIFYQL_PAGE_SIZE || "1000", 10) || 1000,
);

function nowIST() {
  const now = new Date();
  const utc = now.getTime() + now.getTimezoneOffset() * 60000;
  return new Date(utc + 330 * 60000);
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

function todayISTYMD() {
  const d = nowIST();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

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
      `Invalid date format. Expected YYYY-MM-DD. Got start=${startYmd}, end=${endYmd}`,
    );
  }

  const start = ymdToUTCDate(startYmd);
  const end = ymdToUTCDate(endYmd);

  if (start.getTime() > end.getTime()) {
    throw new Error(`Start date is after end date: ${startYmd} > ${endYmd}`);
  }

  const out = [];
  for (let cur = start; cur.getTime() <= end.getTime(); cur = addDaysUTC(cur, 1)) {
    out.push(fmtUTCDateToYMD(cur));
  }
  return out;
}

async function mapWithConcurrency(items, concurrency, mapper) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (true) {
      const idx = nextIndex;
      nextIndex += 1;
      if (idx >= items.length) return;
      results[idx] = await mapper(items[idx], idx);
    }
  }

  const workers = Array.from(
    { length: Math.min(concurrency, items.length) },
    () => worker(),
  );
  await Promise.all(workers);
  return results;
}

async function ensureIndex(conn, tableName, indexName, addIndexSql) {
  const [rows] = await conn.query(`SHOW INDEX FROM ?? WHERE Key_name = ?`, [
    tableName,
    indexName,
  ]);
  if (!rows.length) {
    await conn.query(addIndexSql);
  }
}

async function ensureTablesForBrand(brand) {
  const conn = await brand.pool.getConnection();
  try {
    await conn.query(`
      CREATE TABLE IF NOT EXISTS product_landing_mapping (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        product_id BIGINT NOT NULL,
        landing_page_path VARCHAR(500) NOT NULL,
        status VARCHAR(50) DEFAULT NULL,
        title VARCHAR(255) DEFAULT NULL,
        product_type VARCHAR(255) DEFAULT NULL,
        last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_landing_page_path (landing_page_path(200)),
        KEY idx_product_id (product_id),
        KEY idx_last_synced_at (last_synced_at)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    await conn.query(`
      CREATE TABLE IF NOT EXISTS pipeline_metadata (
        key_name VARCHAR(100) PRIMARY KEY,
        key_value VARCHAR(255) NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    await conn.query(`
      CREATE TABLE IF NOT EXISTS hourly_product_sessions (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        date DATE NOT NULL,
        hour TINYINT UNSIGNED NOT NULL,
        landing_page_type VARCHAR(100) DEFAULT NULL,
        landing_page_path VARCHAR(500) NOT NULL,
        product_id VARCHAR(50) DEFAULT NULL,
        product_title VARCHAR(255) DEFAULT NULL,
        utm_source VARCHAR(255) DEFAULT NULL,
        utm_medium VARCHAR(255) DEFAULT NULL,
        utm_campaign VARCHAR(255) DEFAULT NULL,
        utm_content VARCHAR(255) DEFAULT NULL,
        utm_term VARCHAR(255) DEFAULT NULL,
        referrer_name VARCHAR(255) DEFAULT NULL,
        sessions INT NOT NULL DEFAULT 0,
        sessions_with_cart_additions INT NOT NULL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        KEY idx_date_hour (date, hour),
        KEY idx_product_date (product_id, date),
        KEY idx_date_campaign (date, utm_campaign(150)),
        KEY idx_date_path (date, landing_page_path(200))
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    await conn.query(`
      CREATE TABLE IF NOT EXISTS hourly_product_performance_rollup (
        date DATE NOT NULL,
        hour TINYINT UNSIGNED NOT NULL,
        product_id VARCHAR(50) NOT NULL,
        product_title VARCHAR(255) DEFAULT NULL,
        sessions INT UNSIGNED NOT NULL DEFAULT 0,
        sessions_with_cart_additions INT UNSIGNED NOT NULL DEFAULT 0,
        orders INT UNSIGNED NOT NULL DEFAULT 0,
        units_sold INT UNSIGNED NOT NULL DEFAULT 0,
        total_sales DECIMAL(14,2) NOT NULL DEFAULT 0.00,
        add_to_cart_rate DECIMAL(8,4) NOT NULL DEFAULT 0.0000,
        cvr DECIMAL(8,4) NOT NULL DEFAULT 0.0000,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (date, hour, product_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    await ensureIndex(
      conn,
      "hourly_product_performance_rollup",
      "idx_hpr_product_date_hour",
      "ALTER TABLE hourly_product_performance_rollup ADD INDEX idx_hpr_product_date_hour (product_id, date, hour)",
    );
    await ensureIndex(
      conn,
      "hourly_product_performance_rollup",
      "idx_hpr_date_product",
      "ALTER TABLE hourly_product_performance_rollup ADD INDEX idx_hpr_date_product (date, product_id)",
    );
  } finally {
    conn.release();
  }
}

async function getLastProductSyncDate(brand) {
  const conn = await brand.pool.getConnection();
  try {
    const [rows] = await conn.query(
      `SELECT key_value FROM pipeline_metadata WHERE key_name='last_product_sync_date'`,
    );
    return rows.length ? rows[0].key_value : null;
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
      [dateStr],
    );
  } finally {
    conn.release();
  }
}

async function syncProductsForBrand(brand) {
  const conn = await brand.pool.getConnection();
  try {
    let url = `https://${brand.shopName}.myshopify.com/admin/api/${brand.apiVersion}/products.json?limit=250&fields=id,title,status,handle,product_type`;

    while (url) {
      const resp = await axios.get(url, {
        headers: {
          "X-Shopify-Access-Token": brand.accessToken,
          "Content-Type": "application/json",
        },
        timeout: 60_000,
        validateStatus: () => true,
      });

      if (resp.status === 429) {
        const retry = Number(resp.headers["retry-after"] || "3");
        await new Promise((resolve) => setTimeout(resolve, retry * 1000));
        continue;
      }

      if (resp.status !== 200) {
        throw new Error(`Product sync failed for ${brand.tag}: HTTP ${resp.status}`);
      }

      const products = resp.data.products || [];
      if (!products.length) break;

      const rows = products.map((p) => [
        p.id,
        `/products/${p.handle}`,
        p.status || null,
        p.title || null,
        p.product_type || null,
      ]);

      const placeholders = rows.map(() => "(?, ?, ?, ?, ?)").join(", ");
      await conn.query(
        `
        INSERT INTO product_landing_mapping
          (product_id, landing_page_path, status, title, product_type)
        VALUES ${placeholders}
        ON DUPLICATE KEY UPDATE
          product_id = VALUES(product_id),
          status = VALUES(status),
          title = VALUES(title),
          product_type = VALUES(product_type),
          last_synced_at = CURRENT_TIMESTAMP
        `,
        rows.flat(),
      );

      const link = resp.headers.link || resp.headers.Link;
      if (!link) {
        url = null;
      } else {
        const nextPart = link.split(",").find((entry) => entry.includes('rel="next"'));
        const match = nextPart?.match(/<([^>]+)>/);
        url = match ? match[1] : null;
      }
    }
  } finally {
    conn.release();
  }
}

function formatShopifyQLTable(tableData) {
  const columns = tableData.columns || [];
  const rows = tableData.rows || [];
  const out = [];

  for (const row of rows) {
    if (row && typeof row === "object" && !Array.isArray(row)) {
      out.push({ ...row });
      continue;
    }

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
  return `SINCE ${targetYmd}T00:00:00 UNTIL ${targetYmd}T23:59:59`;
}

function buildShopifyQLHourlyDimensionAllHoursQuery(targetYmd, offset = 0) {
  const dayClause = buildDayClause(targetYmd);
  const tzClause = `WITH TIMEZONE '${SHOPIFYQL_TIMEZONE}'`;
  const pageClause =
    offset > 0
      ? `LIMIT ${SHOPIFYQL_PAGE_SIZE} { OFFSET ${offset} }`
      : `LIMIT ${SHOPIFYQL_PAGE_SIZE}`;

  return `
    FROM sessions
      SHOW
        hour_of_day,
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
        hour_of_day,
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
      ${pageClause}
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
  `.replace(/\n+/g, " ");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getThrottleDelayMs(errors, attempt) {
  const throttledError = (errors || []).find(
    (error) => error?.extensions?.code === "THROTTLED",
  );
  if (!throttledError) return null;

  const resetAt = throttledError.extensions?.cost?.windowResetAt;
  if (resetAt) {
    const resetMs = Date.parse(resetAt) - Date.now();
    if (Number.isFinite(resetMs) && resetMs > 0) {
      return Math.min(resetMs + 1000, 60_000);
    }
  }

  return Math.min(2000 * Math.max(1, attempt), 30_000);
}

async function runShopifyQL(brand, query, label) {
  const url = `https://${brand.shopName}.myshopify.com/admin/api/${brand.apiVersion}/graphql.json`;
  const escaped = query.replace(/"/g, '\\"');
  const graphql = {
    query: `query { shopifyqlQuery(query: "${escaped}") { tableData { rows columns { name } } parseErrors } }`,
  };
  let attempt = 0;

  while (true) {
    attempt += 1;
    const resp = await axios.post(url, graphql, {
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": brand.accessToken,
      },
      timeout: 60_000,
      validateStatus: () => true,
    });

    if (resp.status === 429) {
      const retry = Number(resp.headers["retry-after"] || "3");
      console.log(`[${brand.tag}] ${label} rate-limited, sleeping ${retry}s`);
      await sleep(retry * 1000);
      continue;
    }

    const throttleDelayMs = getThrottleDelayMs(resp.data?.errors, attempt);
    if (throttleDelayMs !== null) {
      console.log(
        `[${brand.tag}] ${label} throttled via GraphQL cost window, sleeping ${Math.ceil(throttleDelayMs / 1000)}s`,
      );
      await sleep(throttleDelayMs);
      continue;
    }

    if (resp.status !== 200 || resp.data?.errors?.length) {
      throw new Error(
        `[${brand.tag}] ${label} failed: HTTP ${resp.status} ${JSON.stringify(resp.data?.errors || [])}`,
      );
    }

    const res = resp.data.data?.shopifyqlQuery;
    if (!res || res.parseErrors?.length) {
      throw new Error(
        `[${brand.tag}] ${label} parseErrors: ${JSON.stringify(res?.parseErrors || [])}`,
      );
    }

    return formatShopifyQLTable(res.tableData);
  }
}

async function fetchHourlyDimensionSessions(brand, targetYmd) {
  const out = [];

  for (let offset = 0; ; offset += SHOPIFYQL_PAGE_SIZE) {
    const pageRows = await runShopifyQL(
      brand,
      buildShopifyQLHourlyDimensionAllHoursQuery(targetYmd, offset),
      `hourly-dim(all, offset=${offset})`,
    );

    out.push(...pageRows);

    if (pageRows.length < SHOPIFYQL_PAGE_SIZE) {
      break;
    }
  }

  return out
    .map((row) => ({
      ...row,
      hour: Number(row.hour_of_day),
    }))
    .filter((row) => Number.isFinite(row.hour) && row.hour >= 0 && row.hour <= 23);
}

async function upsertHourlyProductSessions(brand, rows, targetYmd) {
  const conn = await brand.pool.getConnection();
  try {
    await conn.query(`DELETE FROM hourly_product_sessions WHERE date = ?`, [targetYmd]);

    if (!rows.length) {
      console.log(`[${brand.tag}] No hourly product session rows for ${targetYmd}`);
      return;
    }

    const chunkSize = 500;
    for (let i = 0; i < rows.length; i += chunkSize) {
      const chunk = rows.slice(i, i + chunkSize);
      const insertRows = chunk.map((r) => [
        targetYmd,
        Number(r.hour),
        r.landing_page_type || "Unknown",
        r.landing_page_path || null,
        null,
        null,
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

      const placeholders = insertRows
        .map(() => "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .join(", ");

      await conn.query(
        `
        INSERT INTO hourly_product_sessions
        (
          date, hour, landing_page_type, landing_page_path,
          product_id, product_title,
          utm_source, utm_medium, utm_campaign, utm_content, utm_term,
          referrer_name, sessions, sessions_with_cart_additions, created_at
        )
        VALUES ${placeholders}
        `,
        insertRows.flat(),
      );
    }

    await conn.query(
      `
      UPDATE hourly_product_sessions h
      JOIN product_landing_mapping m
        ON (
          CASE WHEN h.landing_page_path = '/' THEN '/' ELSE TRIM(TRAILING '/' FROM h.landing_page_path) END
        ) = (
          CASE WHEN m.landing_page_path = '/' THEN '/' ELSE TRIM(TRAILING '/' FROM m.landing_page_path) END
        )
      SET h.product_id = m.product_id,
          h.product_title = m.title
      WHERE h.date = ?
        AND (h.product_id IS NULL OR h.product_title IS NULL)
        AND (m.product_id IS NOT NULL OR m.title IS NOT NULL)
      `,
      [targetYmd],
    );
  } finally {
    conn.release();
  }
}

async function upsertHourlyProductPerformanceRollup(brand, targetYmd) {
  const conn = await brand.pool.getConnection();
  try {
    await conn.query(
      `DELETE FROM hourly_product_performance_rollup WHERE date = ?`,
      [targetYmd],
    );

    await conn.query(
      `
      INSERT INTO hourly_product_performance_rollup
      (
        date,
        hour,
        product_id,
        product_title,
        sessions,
        sessions_with_cart_additions,
        orders,
        units_sold,
        total_sales,
        add_to_cart_rate,
        cvr
      )
      SELECT
        k.date,
        k.hour,
        k.product_id,
        COALESCE(s.product_title, o.product_title, 'Unknown') AS product_title,
        COALESCE(s.sessions, 0) AS sessions,
        COALESCE(s.sessions_with_cart_additions, 0) AS sessions_with_cart_additions,
        COALESCE(o.orders, 0) AS orders,
        COALESCE(o.units_sold, 0) AS units_sold,
        COALESCE(o.total_sales, 0.00) AS total_sales,
        ROUND(
          (COALESCE(s.sessions_with_cart_additions, 0) / NULLIF(COALESCE(s.sessions, 0), 0)) * 100,
          4
        ) AS add_to_cart_rate,
        ROUND(
          (COALESCE(o.orders, 0) / NULLIF(COALESCE(s.sessions, 0), 0)) * 100,
          4
        ) AS cvr
      FROM (
        SELECT date, hour, product_id
        FROM (
          SELECT h.date, h.hour, h.product_id
          FROM hourly_product_sessions h
          WHERE h.date = ?
            AND h.product_id IS NOT NULL
            AND h.product_id <> ''
          GROUP BY h.date, h.hour, h.product_id

          UNION

          SELECT o.created_dt AS date, o.created_hr AS hour, o.product_id
          FROM shopify_orders o
          WHERE o.created_dt = ?
            AND o.order_id IS NOT NULL
            AND o.product_id IS NOT NULL
            AND o.product_id <> ''
          GROUP BY o.created_dt, o.created_hr, o.product_id
        ) keys_union
      ) k
      LEFT JOIN (
        SELECT
          h.date,
          h.hour,
          h.product_id,
          MAX(h.product_title) AS product_title,
          SUM(h.sessions) AS sessions,
          SUM(h.sessions_with_cart_additions) AS sessions_with_cart_additions
        FROM hourly_product_sessions h
        WHERE h.date = ?
          AND h.product_id IS NOT NULL
          AND h.product_id <> ''
        GROUP BY h.date, h.hour, h.product_id
      ) s
        ON s.date = k.date
       AND s.hour = k.hour
       AND s.product_id = k.product_id
      LEFT JOIN (
        SELECT
          o.created_dt AS date,
          o.created_hr AS hour,
          o.product_id,
          MAX(o.line_item) AS product_title,
          COUNT(DISTINCT o.order_id) AS orders,
          SUM(GREATEST(COALESCE(o.line_item_quantity, 0), 0)) AS units_sold,
          ROUND(
            SUM(
              (COALESCE(o.line_item_price, 0) * GREATEST(COALESCE(o.line_item_quantity, 0), 0))
              - COALESCE(o.line_item_total_discount, 0)
            ),
            2
          ) AS total_sales
        FROM shopify_orders o
        WHERE o.created_dt = ?
          AND o.order_id IS NOT NULL
          AND o.product_id IS NOT NULL
          AND o.product_id <> ''
        GROUP BY o.created_dt, o.created_hr, o.product_id
      ) o
        ON o.date = k.date
       AND o.hour = k.hour
       AND o.product_id = k.product_id
      `,
      [targetYmd, targetYmd, targetYmd, targetYmd],
    );
  } finally {
    conn.release();
  }
}

async function backfillBrandForDate(brand, targetYmd) {
  const startedAt = Date.now();
  await ensureTablesForBrand(brand);

  const realToday = todayISTYMD();
  const lastSync = await getLastProductSyncDate(brand);
  if (lastSync !== realToday) {
    console.log(`[${brand.tag}] Syncing product mapping before backfill`);
    await syncProductsForBrand(brand);
    await setLastProductSyncDate(brand, realToday);
  }

  const hourlyRows = await fetchHourlyDimensionSessions(brand, targetYmd);
  await upsertHourlyProductSessions(brand, hourlyRows, targetYmd);
  await upsertHourlyProductPerformanceRollup(brand, targetYmd);

  console.log(
    `[${brand.tag}] Backfilled ${targetYmd} in ${((Date.now() - startedAt) / 1000).toFixed(1)}s`,
  );
}

async function main() {
  if (!BACKFILL_START_IST_DATE || !BACKFILL_END_IST_DATE) {
    throw new Error(
      "BACKFILL_START_IST_DATE and BACKFILL_END_IST_DATE are required",
    );
  }

  const dates = buildInclusiveDateRangeYMD(
    BACKFILL_START_IST_DATE,
    BACKFILL_END_IST_DATE,
  );
  const brands = await loadPipelineBrandsForScripts();

  if (!brands.length) {
    throw new Error("No brands configured");
  }

  console.log(
    `[BACKFILL] Starting hourly product backfill @ ${fmtIST()} for ${brands.length} brand(s)`,
  );
  console.log(
    `[BACKFILL] Range: ${dates[0]} -> ${dates[dates.length - 1]} (${dates.length} day(s))`,
  );

  try {
    for (const targetYmd of dates) {
      console.log(`[BACKFILL] Processing ${targetYmd}`);
      await mapWithConcurrency(brands, BACKFILL_BRAND_CONCURRENCY, (brand) =>
        backfillBrandForDate(brand, targetYmd),
      );
    }
  } finally {
    await Promise.all(brands.map((brand) => brand.pool.end()));
  }

  console.log(`[BACKFILL] Completed @ ${fmtIST()}`);
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("[BACKFILL] Failed:", err);
    process.exit(1);
  });
