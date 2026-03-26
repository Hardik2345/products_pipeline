import mysql from "mysql2/promise";
import axios from "axios";
import crypto from "node:crypto";
import fs from "node:fs";

function normalizeKey(rawKey) {
  let buf = Buffer.from(String(rawKey || ""), "utf8");
  if (buf.length < 32) {
    const padded = Buffer.alloc(32);
    buf.copy(padded);
    buf = padded;
  } else if (buf.length > 32) {
    buf = buf.slice(0, 32);
  }
  return buf;
}

function decryptAES(encryptedValue, passwordAesKey) {
  if (!encryptedValue) return encryptedValue;
  if (!String(encryptedValue).includes(":")) return encryptedValue;

  const [ivB64, encryptedB64] = String(encryptedValue).split(":");
  const iv = Buffer.from(ivB64, "base64");
  const encrypted = Buffer.from(encryptedB64, "base64");
  const key = normalizeKey(passwordAesKey);

  const decipher = crypto.createDecipheriv("aes-256-cbc", key, iv);
  let decrypted = decipher.update(encrypted, undefined, "utf8");
  decrypted += decipher.final("utf8");
  return decrypted;
}

function buildSslOptions({ dbHost }) {
  const testMode = String(process.env.TEST_MODE || "").toLowerCase().trim() === "true";
  const dbSslEnabledEnv = String(process.env.DB_SSL_ENABLED || "").toLowerCase().trim();
  let dbSslEnabled = dbSslEnabledEnv === "true";

  if (
    !dbSslEnabled &&
    dbHost &&
    dbSslEnabledEnv === "" &&
    /amazonaws\.com$/i.test(dbHost) &&
    !testMode
  ) {
    dbSslEnabled = true;
  }

  const dbSslCa = process.env.DB_SSL_CA;
  const dbSslCaFile = process.env.DB_SSL_CA_FILE;

  let ssl;
  if (dbSslEnabled || dbSslCa || dbSslCaFile) {
    let ca = dbSslCa ? String(dbSslCa).replace(/\\n/g, "\n") : undefined;
    if (!ca && dbSslCaFile) {
      try {
        ca = fs.readFileSync(dbSslCaFile, "utf8");
      } catch (err) {
        console.warn(`[BRAND LOADER] Failed to read DB_SSL_CA_FILE (${dbSslCaFile}): ${err?.message}`);
      }
    }

    ssl = {
      rejectUnauthorized: false,
      ...(ca ? { ca } : {}),
    };
  }

  return { ssl, testMode };
}

function loadBrandsFromIndexedEnv() {
  const count = parseInt(process.env.TOTAL_CONFIG_COUNT || "0", 10);
  const brands = [];

  for (let i = 0; i < count; i++) {
    const dbHost = process.env[`DB_HOST_${i}`];
    const dbUser = process.env[`DB_USER_${i}`];
    const dbPassword = process.env[`DB_PASSWORD_${i}`];
    const dbDatabase = process.env[`DB_DATABASE_${i}`];
    const brandTag = process.env[`BRAND_TAG_${i}`] || `brand_${i}`;

    if (!dbHost || !dbUser || !dbDatabase) {
      console.warn(`[BRAND LOADER] Skipping brand index ${i} – missing DB config.`);
      continue;
    }

    const { ssl, testMode } = buildSslOptions({ dbHost });

    const pool = mysql.createPool({
      host: dbHost,
      user: dbUser,
      password: dbPassword,
      database: dbDatabase,
      waitForConnections: true,
      connectionLimit: 3,
      queueLimit: 0,
      ...(ssl && !testMode ? { ssl } : {}),
    });

    brands.push({ index: i, tag: brandTag, pool, dbDatabase });
  }

  return brands;
}

async function loadBrandsFromApi() {
  const GET_BRANDS_API = process.env.GET_BRANDS_API;
  const PIPELINE_AUTH_HEADER = process.env.PIPELINE_AUTH_HEADER;
  const PASSWORD_AES_KEY = process.env.PASSWORD_AES_KEY;

  if (!GET_BRANDS_API || !PIPELINE_AUTH_HEADER || !PASSWORD_AES_KEY) {
    return null;
  }

  const API_HEADERS = { "x-pipeline-key": PIPELINE_AUTH_HEADER };

  console.log(`[BRAND LOADER] Fetching brands from GET_BRANDS_API=${GET_BRANDS_API}`);

  let brandDict = {};
  try {
    const resp = await axios.get(GET_BRANDS_API, {
      headers: API_HEADERS,
      timeout: 30_000,
      validateStatus: () => true,
    });

    if (resp.status !== 200) {
      const hint =
        resp.status === 401
          ? " (401 Unauthorized: check PIPELINE_AUTH_HEADER / x-pipeline-key secret)"
          : "";
      throw new Error(
        `[BRAND LOADER] Brand API request failed: HTTP ${resp.status}${hint}`,
      );
    }

    brandDict = resp.data || {};
  } catch (err) {
    throw new Error(`[BRAND LOADER] Failed to fetch brand list: ${err.message}`);
  }

  const brandIds = Object.keys(brandDict);
  if (brandIds.length === 0) {
    console.warn("[BRAND LOADER] No brands returned by GET_BRANDS_API");
    return [];
  }

  const brands = [];
  let index = 0;

  for (const strId of brandIds) {
    const id = parseInt(strId, 10);
    if (!Number.isFinite(id)) continue;

    const brandUrl = `${GET_BRANDS_API.replace(/\/+$/, "")}/${id}`;
    let brandData;
    try {
      const resp = await axios.get(brandUrl, {
        headers: API_HEADERS,
        timeout: 30_000,
        validateStatus: () => true,
      });
      if (resp.status !== 200) {
        const hint =
          resp.status === 401
            ? " (401 Unauthorized: check PIPELINE_AUTH_HEADER / x-pipeline-key secret)"
            : "";
        console.warn(
          `[BRAND LOADER] Failed to fetch credentials for brand ID ${id}: HTTP ${resp.status}${hint}`,
        );
        continue;
      }
      brandData = resp.data || {};
    } catch (err) {
      console.warn(`[BRAND LOADER] Failed to fetch credentials for brand ID ${id}: ${err.message}`);
      continue;
    }

    const dbHost = brandData.db_host;
    const dbUser = brandData.db_user;
    const dbDatabase = brandData.db_database;
    const brandTag = brandData.brand_tag || `brand_${index}`;

    let dbPassword;
    try {
      dbPassword = decryptAES(brandData.db_password, PASSWORD_AES_KEY);
    } catch (err) {
      console.warn(`[BRAND LOADER] Skipping ${brandTag} – failed to decrypt DB password.`);
      continue;
    }

    if (!dbHost || !dbUser || !dbDatabase) {
      console.warn(`[BRAND LOADER] Skipping ${brandTag} – missing required DB fields.`);
      continue;
    }

    const { ssl, testMode } = buildSslOptions({ dbHost });

    const pool = mysql.createPool({
      host: dbHost,
      user: dbUser,
      password: dbPassword,
      database: dbDatabase,
      waitForConnections: true,
      connectionLimit: 3,
      queueLimit: 0,
      ...(ssl && !testMode ? { ssl } : {}),
    });

    brands.push({ index, id, tag: brandTag, pool, dbDatabase });
    index++;
  }

  return brands;
}

async function loadPipelineBrandsFromApi() {
  const GET_BRANDS_API = process.env.GET_BRANDS_API;
  const PIPELINE_AUTH_HEADER = process.env.PIPELINE_AUTH_HEADER;
  const PASSWORD_AES_KEY = process.env.PASSWORD_AES_KEY;

  if (!GET_BRANDS_API || !PIPELINE_AUTH_HEADER || !PASSWORD_AES_KEY) {
    return null;
  }

  const API_HEADERS = { "x-pipeline-key": PIPELINE_AUTH_HEADER };

  console.log(`[BRAND LOADER] Fetching pipeline brands from GET_BRANDS_API=${GET_BRANDS_API}`);

  let brandDict = {};
  try {
    const resp = await axios.get(GET_BRANDS_API, {
      headers: API_HEADERS,
      timeout: 30_000,
      validateStatus: () => true,
    });

    if (resp.status !== 200) {
      const hint =
        resp.status === 401
          ? " (401 Unauthorized: check PIPELINE_AUTH_HEADER / x-pipeline-key secret)"
          : "";
      throw new Error(
        `[BRAND LOADER] Pipeline brand API request failed: HTTP ${resp.status}${hint}`,
      );
    }

    brandDict = resp.data || {};
  } catch (err) {
    throw new Error(`[BRAND LOADER] Failed to fetch pipeline brand list: ${err.message}`);
  }

  const brandIds = Object.keys(brandDict);
  if (brandIds.length === 0) {
    console.warn("[BRAND LOADER] No pipeline brands returned by GET_BRANDS_API");
    return [];
  }

  const brands = [];
  let index = 0;

  for (const strId of brandIds) {
    const id = parseInt(strId, 10);
    if (!Number.isFinite(id)) continue;

    const brandUrl = `${GET_BRANDS_API.replace(/\/+$/, "")}/${id}`;
    let brandData;
    try {
      const resp = await axios.get(brandUrl, {
        headers: API_HEADERS,
        timeout: 30_000,
        validateStatus: () => true,
      });
      if (resp.status !== 200) {
        const hint =
          resp.status === 401
            ? " (401 Unauthorized: check PIPELINE_AUTH_HEADER / x-pipeline-key secret)"
            : "";
        console.warn(
          `[BRAND LOADER] Failed to fetch pipeline credentials for brand ID ${id}: HTTP ${resp.status}${hint}`,
        );
        continue;
      }
      brandData = resp.data || {};
    } catch (err) {
      console.warn(`[BRAND LOADER] Failed to fetch pipeline credentials for brand ID ${id}: ${err.message}`);
      continue;
    }

    const shopName = brandData.shop_name;
    const apiVersion = brandData.api_version;
    const dbHost = brandData.db_host;
    const dbUser = brandData.db_user;
    const dbDatabase = brandData.db_database;
    const brandTag = brandData.brand_tag || `brand_${index}`;
    const brandName = brandData.brand_name || brandTag.toUpperCase();

    let accessToken;
    let dbPassword;
    try {
      accessToken = decryptAES(brandData.access_token, PASSWORD_AES_KEY);
      dbPassword = decryptAES(brandData.db_password, PASSWORD_AES_KEY);
    } catch (err) {
      console.warn(`[BRAND LOADER] Skipping ${brandTag} – failed to decrypt credentials.`);
      continue;
    }

    if (!shopName || !apiVersion || !accessToken || !dbHost || !dbUser || !dbDatabase) {
      console.warn(`[BRAND LOADER] Skipping ${brandTag} – missing required pipeline fields.`);
      continue;
    }

    const { ssl, testMode } = buildSslOptions({ dbHost });

    const pool = mysql.createPool({
      host: dbHost,
      user: dbUser,
      password: dbPassword,
      database: dbDatabase,
      waitForConnections: true,
      connectionLimit: 3,
      queueLimit: 0,
      ...(ssl && !testMode ? { ssl } : {}),
    });

    brands.push({
      index,
      id,
      tag: brandTag,
      name: brandName,
      shopName,
      apiVersion,
      accessToken,
      dbDatabase,
      pool,
    });
    index++;
  }

  return brands;
}

export async function loadBrandsForScripts() {
  const hasApiMode =
    !!process.env.GET_BRANDS_API &&
    !!process.env.PIPELINE_AUTH_HEADER &&
    !!process.env.PASSWORD_AES_KEY;

  if (!hasApiMode) {
    throw new Error(
      "[BRAND LOADER] API mode is required for scripts. Missing one of: GET_BRANDS_API, PIPELINE_AUTH_HEADER, PASSWORD_AES_KEY",
    );
  }

  const apiBrands = await loadBrandsFromApi();
  if (Array.isArray(apiBrands) && apiBrands.length > 0) {
    console.log(`[BRAND LOADER] Loaded ${apiBrands.length} brand(s) from API`);
    return apiBrands;
  }

  throw new Error(
    "[BRAND LOADER] API mode succeeded but returned 0 brands. Check GET_BRANDS_API response.",
  );
}

export async function loadPipelineBrandsForScripts() {
  const hasApiMode =
    !!process.env.GET_BRANDS_API &&
    !!process.env.PIPELINE_AUTH_HEADER &&
    !!process.env.PASSWORD_AES_KEY;

  if (!hasApiMode) {
    throw new Error(
      "[BRAND LOADER] API mode is required for pipeline scripts. Missing one of: GET_BRANDS_API, PIPELINE_AUTH_HEADER, PASSWORD_AES_KEY",
    );
  }

  const apiBrands = await loadPipelineBrandsFromApi();
  if (Array.isArray(apiBrands) && apiBrands.length > 0) {
    console.log(`[BRAND LOADER] Loaded ${apiBrands.length} pipeline brand(s) from API`);
    return apiBrands;
  }

  throw new Error(
    "[BRAND LOADER] Pipeline API mode succeeded but returned 0 brands. Check GET_BRANDS_API response.",
  );
}
