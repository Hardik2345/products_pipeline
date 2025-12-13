import 'dotenv/config';
import mysql from 'mysql2/promise';
import axios from 'axios';

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
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const hh = String(d.getHours()).padStart(2, '0');
  const mi = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}T${hh}:${mi}:${ss}+05:30`;
}

function todayISTDate() {
  const d = nowIST();
  return new Date(d.getFullYear(), d.getMonth(), d.getDate());
}

function fmtDate(d) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

// ---------- Brand config ----------
function loadBrands() {
  const count = parseInt(process.env.TOTAL_CONFIG_COUNT || '0', 10);
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
    });

    console.log(`[INIT] Brand[${i}] ${brandName} ready (shop=${shopName}, db=${dbDatabase})`);
  }

  console.log(`[INIT] Active brands: ${brands.map((b) => `${b.index}:${b.name}`).join(', ')}`);
  return brands;
}

// ---------- DB Setup ----------
async function ensureTablesForBrand(brand) {
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

    await conn.query(`
      CREATE TABLE IF NOT EXISTS mv_product_sessions_by_path_daily (
        date DATE NOT NULL,
        landing_page_path VARCHAR(500) NOT NULL,
        product_id VARCHAR(50) DEFAULT NULL,
        sessions INT NOT NULL DEFAULT 0,
        sessions_with_cart_additions INT NOT NULL DEFAULT 0,
        add_to_cart_rate DECIMAL(6,4) NOT NULL DEFAULT 0,
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

    await conn.query(`
      CREATE TABLE IF NOT EXISTS mv_product_sessions_by_campaign_daily (
        date DATE NOT NULL,
        utm_campaign VARCHAR(255) NOT NULL,
        product_id   VARCHAR(50)  DEFAULT NULL,
        referrer_name VARCHAR(255) DEFAULT NULL,
        sessions INT NOT NULL DEFAULT 0,
        sessions_with_cart_additions INT NOT NULL DEFAULT 0,
        add_to_cart_rate_pct   DECIMAL(7,4) NOT NULL DEFAULT 0,
        conversion_rate_pct    DECIMAL(7,4) NOT NULL DEFAULT 0,
        PRIMARY KEY (date, utm_campaign, product_id, referrer_name(100)),
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
          'X-Shopify-Access-Token': brand.accessToken,
          'Content-Type': 'application/json',
        },
        validateStatus: () => true,
      });

      if (resp.status === 429) {
        const retry = Number(resp.headers['retry-after'] || '3');
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

      const rows = products.map((p) => [
        p.id,
        `/products/${p.handle}`,
        p.status || null,
        p.title || null,
      ]);

      if (rows.length) {
        const placeholders = rows.map(() => '(?, ?, ?, ?)').join(', ');
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
        const nextPart = link.split(',').find((l) => l.includes('rel="next"'));
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

// ---------- ShopifyQL ----------
function buildShopifyQLQuery() {
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
      DURING today
      ORDER BY sessions DESC
      LIMIT 1000
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
  `.replace(/\n+/g, ' ');
}

function formatShopifyQLTable(tableData) {
  const columns = tableData.columns || [];
  const rows = tableData.rows || [];
  const out = [];

  for (const row of rows) {
    // Case 1: NEW STYLE (object rows)
    if (row && typeof row === 'object' && !Array.isArray(row)) {
      out.push({ ...row });
      continue;
    }

    // Case 2: LEGACY STYLE (array rows)
    if (Array.isArray(row)) {
      const obj = {};
      row.forEach((val, idx) => {
        if (columns[idx]) {
          obj[columns[idx].name] = val;
        }
      });
      out.push(obj);
      continue;
    }
  }

  return out;
}

async function fetchShopifyQLSessions(brand) {
  const url = `https://${brand.shopName}.myshopify.com/admin/api/2025-10/graphql.json`;
  const q = buildShopifyQLQuery().replace(/"/g, '\\"');

  const graphql = {
    query: `query { shopifyqlQuery(query: "${q}") { tableData { rows columns { name } } parseErrors } }`,
  };

  const resp = await axios.post(url, graphql, {
    headers: {
      'Content-Type': 'application/json',
      'X-Shopify-Access-Token': brand.accessToken,
    },
    timeout: 60000,
    validateStatus: () => true,
  });

  if (resp.status !== 200 || resp.data.errors) return [];

  const res = resp.data.data.shopifyqlQuery;
  if (res.parseErrors?.length) return [];

  return formatShopifyQLTable(res.tableData);
}

// ---------- Snapshot + MV refresh ----------
async function upsertProductSessionsSnapshot(brand, rows) {
  const today = fmtDate(todayISTDate());
  const conn = await brand.pool.getConnection();

  try {
    await conn.query(`DELETE FROM product_sessions_snapshot WHERE date=?`, [today]);

    if (!rows.length) return;

    const insertRows = rows.map((r) => [
      today,
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

    const placeholders = insertRows.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').join(', ');

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

    console.log(`[${brand.name}] Inserted ${insertRows.length} rows into snapshot.`);
  } finally {
    conn.release();
  }
}

async function refreshMaterializedViews(brand) {
  const today = fmtDate(todayISTDate());
  const conn = await brand.pool.getConnection();

  try {
    // Wipe today's rows from all MVs
    await conn.query(`DELETE FROM mv_product_sessions_by_type_daily     WHERE date = ?`, [today]);
    await conn.query(`DELETE FROM mv_product_sessions_by_path_daily     WHERE date = ?`, [today]);
    await conn.query(`DELETE FROM mv_product_sessions_by_campaign_daily WHERE date = ?`, [today]);

    // ---------- TYPE MV (same as before) ----------
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
      [today]
    );

    // ---------- PATH MV (product-level, with % + conversion) ----------
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

        -- raw ATC rate (0–1)
        CASE
          WHEN SUM(s.sessions) > 0
            THEN ROUND(SUM(s.sessions_with_cart_additions) / SUM(s.sessions), 4)
          ELSE 0
        END AS add_to_cart_rate,

        -- ATC % = ATC sessions / sessions * 100
        CASE
          WHEN SUM(s.sessions) > 0
            THEN ROUND(SUM(s.sessions_with_cart_additions) / SUM(s.sessions) * 100, 4)
          ELSE 0
        END AS add_to_cart_rate_pct,

        -- conversion % = orders / sessions * 100
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
      [today, today]
    );

    // ---------- CAMPAIGN MV (campaign + path + product + referrer) ----------
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

        -- ATC % = ATC sessions / sessions * 100
        CASE
          WHEN SUM(s.sessions) > 0
            THEN ROUND(SUM(s.sessions_with_cart_additions) / SUM(s.sessions) * 100, 4)
          ELSE 0
        END AS add_to_cart_rate_pct,

        -- conversion % = orders / sessions * 100
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
      [today, today]
    );

    console.log(`[${brand.name}] Refreshed MVs for ${today}`);
  } finally {
    conn.release();
  }
}

// ---------- Pipeline per brand ----------
async function processBrand(brand) {
  console.log(`\n========== ${brand.tag} ==========\n`);

  await ensureTablesForBrand(brand);

  const today = fmtDate(todayISTDate());
  const lastSync = await getLastProductSyncDate(brand);

  if (lastSync !== today) {
    await syncProductsForBrand(brand);
    await setLastProductSyncDate(brand, today);
  } else {
    console.log(`[${brand.tag}] Product sync already done today, skipping.`);
  }

  const rows = await fetchShopifyQLSessions(brand);
  await upsertProductSessionsSnapshot(brand, rows);
  await refreshMaterializedViews(brand);

  console.log(`[${brand.tag}] Pipeline complete.`);
}

// ---------- Main ----------
async function runPipeline() {
  const brands = loadBrands();

  console.log(`\n🚀 Product Sessions Pipeline @ ${fmtIST()}\n`);

  await Promise.all(brands.map((b) => processBrand(b)));

  console.log(`\n✅ All brands completed.\n`);
}

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
    await runPipeline();
    console.log(`\n[SCHED] Pipeline completed (${trigger}) @ ${fmtIST()}\n`);
  } catch (err) {
    console.error(`[SCHED] Pipeline crashed (${trigger}) @ ${fmtIST()}:`, err);
  } finally {
    running = false;
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  // 1) Run immediately on deployment/startup
  safeRun("startup");

  // 2) Cron: run at the start of each hour (minute 0, second 0) in Asia/Kolkata
  cron.schedule(
    "0 0 * * * *",           // second minute hour day month weekday
    () => safeRun("hourly"),
    {
      timezone: "Asia/Kolkata",
    }
  );

  console.log(`[SCHED] Cron enabled: runs at start of every hour (Asia/Kolkata).`);
  console.log(`[SCHED] Service started @ ${fmtIST()}`);
}
