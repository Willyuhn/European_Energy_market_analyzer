const path = require("path");
const fs = require("fs");
const os = require("os");
const { spawn } = require("child_process");
const cron = require("node-cron");

// Load local .env and OVERRIDE host-injected env (e.g. Hostinger's Node app config).
// Checks the app dir first, then ~/enerlyzer-etl/.env (outside the deploy dir, so it
// survives redeploys). Deliberately overrides process.env so stale injected values
// (old DB host/user) can't win over the intended local config.
function etlDirCandidates() {
  const home = process.env.HOME || process.env.USERPROFILE || os.homedir();
  const user = process.env.USER || process.env.LOGNAME;
  const dirs = [
    path.join(__dirname, "../../../enerlyzer-etl"),
    path.join(home, "enerlyzer-etl"),
  ];
  if (user) dirs.push(path.join("/home", user, "enerlyzer-etl"));
  return dirs;
}

(() => {
  try {
    const candidates = [
      path.join(__dirname, ".env"),
      ...etlDirCandidates().map((d) => path.join(d, ".env")),
    ];
    for (const p of candidates) {
      if (!fs.existsSync(p)) continue;
      for (const line of fs.readFileSync(p, "utf8").split("\n")) {
        const m = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/);
        if (!m) continue;
        let v = m[2];
        if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
          v = v.slice(1, -1);
        }
        process.env[m[1]] = v;
      }
    }
  } catch (e) {
    console.error("[enerlyzer] env loader failed:", e.message);
  }
})();
const express = require("express");
const mysql = require("mysql2/promise");

function requiredEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required environment variable: ${name}`);
  return v;
}

const PORT = parseInt(process.env.PORT || "8080", 10);

const dbConfig = {
  host: requiredEnv("DB_HOST"),
  port: parseInt(process.env.DB_PORT || "3306", 10),
  user: requiredEnv("DB_USER"),
  password: requiredEnv("DB_PASSWORD"),
  database: process.env.DB_NAME || "energy_market",
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
};

const pool = mysql.createPool(dbConfig);

const app = express();

app.disable("x-powered-by");

// Static frontend
app.use("/", express.static(path.join(__dirname, "public"), { extensions: ["html"] }));

// Optional static assets folder if you add one later (e.g. images)
app.use("/static", express.static(path.join(__dirname, "static")));

app.get("/health", async (_req, res) => {
  try {
    const conn = await pool.getConnection();
    await conn.ping();
    conn.release();
    res.json({ status: "healthy", database: "connected" });
  } catch (e) {
    res.status(500).json({ status: "unhealthy", error: String(e?.message || e) });
  }
});

app.get("/api/summary/total", async (_req, res) => {
  try {
    const [rows] = await pool.query(
      "SELECT total_neg_hours, avg_market_price, capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct FROM summary_total WHERE id = 1"
    );
    const row = rows?.[0] || {};
    res.json({
      neg_hours: Number(row.total_neg_hours || 0),
      avg_market_price: Number(row.avg_market_price || 0),
      capture_price: Number(row.capture_price || 0),
      capture_price_floor0: Number(row.capture_price_floor0 || 0),
      capture_rate: Number(row.capture_rate || 0),
      solar_at_neg_price_pct: Number(row.solar_at_neg_price_pct || 0)
    });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

app.get("/api/summary/yearly", async (req, res) => {
  try {
    const year = req.query.year ? parseInt(String(req.query.year), 10) : null;
    const sql = year
      ? "SELECT country, total_neg_hours, avg_market_price, capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct FROM summary_yearly WHERE year = ? ORDER BY country"
      : "SELECT country, total_neg_hours, avg_market_price, capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct FROM summary_yearly ORDER BY country";
    const params = year ? [year] : [];
    const [rows] = await pool.query(sql, params);
    const data = (rows || []).map((r) => ({
      country: r.country,
      neg_hours: Number(r.total_neg_hours || 0),
      avg_market_price: Number(r.avg_market_price || 0),
      capture_price: Number(r.capture_price || 0),
      capture_price_floor0: Number(r.capture_price_floor0 || 0),
      capture_rate: Number(r.capture_rate || 0),
      solar_at_neg_price_pct: Number(r.solar_at_neg_price_pct || 0)
    }));
    res.json({ data });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

app.get("/api/summary/years", async (_req, res) => {
  try {
    // Prefer summary_monthly.year if present; fallback to YEAR(DateTime(UTC)) from energy_prices
    let years = [];
    try {
      const [rows] = await pool.query("SELECT DISTINCT year FROM summary_monthly ORDER BY year DESC");
      years = (rows || []).map((r) => Number(r.year)).filter(Boolean);
    } catch {
      const [rows] = await pool.query("SELECT DISTINCT YEAR(`DateTime(UTC)`) AS year FROM energy_prices ORDER BY year DESC");
      years = (rows || []).map((r) => Number(r.year)).filter(Boolean);
    }
    if (!years.length) years = [new Date().getFullYear()];
    res.json({ years });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

async function tableHasColumn(table, column) {
  const [rows] = await pool.query("SHOW COLUMNS FROM ?? LIKE ?", [table, column]);
  return Array.isArray(rows) && rows.length > 0;
}

app.get("/api/summary/monthly", async (req, res) => {
  try {
    const year = req.query.year ? parseInt(String(req.query.year), 10) : new Date().getFullYear();
    const hasYear = await tableHasColumn("summary_monthly", "year");
    let rows;
    if (hasYear) {
      [rows] = await pool.query(
        "SELECT country, month, year, neg_hours, avg_market_price, capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct FROM summary_monthly WHERE year = ? ORDER BY country, month",
        [year]
      );
    } else {
      [rows] = await pool.query(
        "SELECT country, month, neg_hours, avg_market_price, capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct FROM summary_monthly ORDER BY country, month"
      );
    }
    const data = (rows || []).map((r) => ({
      country: r.country,
      month: Number(r.month),
      year: hasYear ? Number(r.year) : year,
      neg_hours: Number((hasYear ? r.neg_hours : r.neg_hours) || 0),
      avg_market_price: Number(r.avg_market_price || 0),
      capture_price: Number(r.capture_price || 0),
      capture_price_floor0: Number(r.capture_price_floor0 || 0),
      capture_rate: Number(r.capture_rate || 0),
      solar_at_neg_price_pct: Number(r.solar_at_neg_price_pct || 0)
    }));
    res.json({ data });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

app.get("/api/summary/daily", async (req, res) => {
  try {
    const country = req.query.country ? String(req.query.country) : null;
    const month = req.query.month ? parseInt(String(req.query.month), 10) : null;
    const year = req.query.year ? parseInt(String(req.query.year), 10) : new Date().getFullYear();

    const hasYear = await tableHasColumn("summary_daily", "year");

    let sql;
    let params = [];
    if (hasYear) {
      if (country && month) {
        sql =
          "SELECT country, month, year, day, neg_hours, avg_market_price, capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct FROM summary_daily WHERE country = ? AND month = ? AND year = ? ORDER BY day";
        params = [country, month, year];
      } else {
        sql =
          "SELECT country, month, year, day, neg_hours, avg_market_price, capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct FROM summary_daily WHERE year = ? ORDER BY country, month, day";
        params = [year];
      }
    } else {
      if (country && month) {
        sql =
          "SELECT country, month, day, neg_hours, avg_market_price, capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct FROM summary_daily WHERE country = ? AND month = ? ORDER BY day";
        params = [country, month];
      } else {
        sql =
          "SELECT country, month, day, neg_hours, avg_market_price, capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct FROM summary_daily ORDER BY country, month, day";
      }
    }

    const [rows] = await pool.query(sql, params);
    const data = (rows || []).map((r) => {
      if (hasYear) {
        return {
          country: r.country,
          month: Number(r.month),
          year: Number(r.year),
          day: Number(r.day),
          neg_hours: Number(r.neg_hours || 0),
          avg_market_price: Number(r.avg_market_price || 0),
          capture_price: Number(r.capture_price || 0),
          capture_price_floor0: Number(r.capture_price_floor0 || 0),
          capture_rate: Number(r.capture_rate || 0),
          solar_at_neg_price_pct: Number(r.solar_at_neg_price_pct || 0)
        };
      }
      return {
        country: r.country,
        month: Number(r.month),
        year,
        day: Number(r.day),
        neg_hours: Number(r.neg_hours || 0),
        avg_market_price: Number(r.avg_market_price || 0),
        capture_price: Number(r.capture_price || 0),
        capture_price_floor0: Number(r.capture_price_floor0 || 0),
        capture_rate: Number(r.capture_rate || 0),
        solar_at_neg_price_pct: Number(r.solar_at_neg_price_pct || 0)
      };
    });
    res.json({ data });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

app.get("/api/hourly/day", async (req, res) => {
  try {
    const country = requiredQuery(req, "country");
    const month = parseInt(requiredQuery(req, "month"), 10);
    const day = parseInt(requiredQuery(req, "day"), 10);
    const year = parseInt(requiredQuery(req, "year"), 10);

    const dateStr = `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;

    const [areaRows] = await pool.query(
      "SELECT DISTINCT AreaCode FROM energy_prices WHERE AreaDisplayName = ? AND `Sequence` = '1' LIMIT 1",
      [country]
    );
    const areaCode = areaRows?.[0]?.AreaCode || null;

    const priceSql = areaCode
      ? "SELECT `DateTime(UTC)` AS dt, `Price[Currency/MWh]` AS price, ResolutionCode AS res FROM energy_prices WHERE AreaCode = ? AND DATE(`DateTime(UTC)`) = ? AND `Sequence` = '1' ORDER BY `DateTime(UTC)`"
      : "SELECT `DateTime(UTC)` AS dt, `Price[Currency/MWh]` AS price, ResolutionCode AS res FROM energy_prices WHERE AreaDisplayName = ? AND DATE(`DateTime(UTC)`) = ? AND `Sequence` = '1' ORDER BY `DateTime(UTC)`";
    const priceParams = areaCode ? [areaCode, dateStr] : [country, dateStr];
    const [priceRows] = await pool.query(priceSql, priceParams);

    const solarSql = areaCode
      ? "SELECT `DateTime(UTC)` AS dt, ActualGenerationOutput AS gen FROM generation_per_type WHERE AreaCode = ? AND DATE(`DateTime(UTC)`) = ? AND ProductionType = 'Solar' ORDER BY `DateTime(UTC)`"
      : "SELECT `DateTime(UTC)` AS dt, ActualGenerationOutput AS gen FROM generation_per_type WHERE AreaDisplayName = ? AND DATE(`DateTime(UTC)`) = ? AND ProductionType = 'Solar' ORDER BY `DateTime(UTC)`";
    const solarParams = areaCode ? [areaCode, dateStr] : [country, dateStr];
    const [solarRows] = await pool.query(solarSql, solarParams);

    const prices = new Map();
    for (const r of priceRows || []) {
      const ts = normalizeTs(r.dt);
      prices.set(ts, Number(r.price || 0));
    }

    const solar = new Map();
    for (const r of solarRows || []) {
      const ts = normalizeTs(r.dt);
      solar.set(ts, Number(r.gen || 0));
    }

    // Build 15-minute grid (96 points)
    const start = new Date(`${dateStr}T00:00:00Z`);
    const all = [];
    for (let i = 0; i < 96; i++) {
      const d = new Date(start.getTime() + i * 15 * 60 * 1000);
      all.push(toSqlTs(d));
    }

    const sortedPriceKeys = Array.from(prices.keys()).sort();
    const sortedSolarKeys = Array.from(solar.keys()).sort();

    let lastPrice = null;
    let lastSolar = 0;

    const result = [];
    for (const ts of all) {
      if (prices.has(ts)) lastPrice = prices.get(ts);
      else if (lastPrice === null) {
        for (const k of sortedPriceKeys) {
          if (k <= ts) lastPrice = prices.get(k);
          else break;
        }
      }

      if (solar.has(ts)) lastSolar = solar.get(ts);
      else {
        let found = false;
        for (const k of sortedSolarKeys) {
          if (k <= ts) {
            lastSolar = solar.get(k);
            found = true;
          } else break;
        }
        if (!found) lastSolar = 0;
      }

      result.push({ timestamp: ts, price: lastPrice ?? 0, solar_mw: lastSolar });
    }

    res.json({ data: result, country, date: dateStr });
  } catch (e) {
    res.status(400).json({ error: String(e?.message || e) });
  }
});

function requiredQuery(req, name) {
  const v = req.query[name];
  if (v === undefined || v === null || v === "") throw new Error(`Missing query parameter: ${name}`);
  return String(v);
}

function normalizeTs(dt) {
  // dt can be JS Date or string; normalize to "YYYY-MM-DD HH:MM:SS"
  if (dt instanceof Date) return toSqlTs(dt);
  const s = String(dt);
  if (s.includes("T")) return s.replace("T", " ").replace("Z", "").slice(0, 19);
  return s.slice(0, 19);
}

function toSqlTs(date) {
  const pad = (n) => String(n).padStart(2, "0");
  return (
    date.getUTCFullYear() +
    "-" +
    pad(date.getUTCMonth() + 1) +
    "-" +
    pad(date.getUTCDate()) +
    " " +
    pad(date.getUTCHours()) +
    ":" +
    pad(date.getUTCMinutes()) +
    ":" +
    pad(date.getUTCSeconds())
  );
}

/* ---------- daily ENTSO-E update (Hostinger Node apps have no hPanel cron) ---------- */
# Primary fire at 06:00 Europe/Berlin. Catch-up every 30 min after 06:00 until
# daily.sh writes logs/daily_ok_YYYYMMDD (survives process restarts / missed ticks).
const DAILY_CRON_EXPR = process.env.DAILY_CRON_EXPR || "0 6 * * *";
const DAILY_CATCHUP_EXPR = process.env.DAILY_CATCHUP_EXPR || "*/30 6-23 * * *";
const DAILY_CRON_TZ = process.env.DAILY_CRON_TZ || "Europe/Berlin";
let dailyCronStatus = {
  enabled: false,
  expr: DAILY_CRON_EXPR,
  catchupExpr: DAILY_CATCHUP_EXPR,
  tz: DAILY_CRON_TZ,
  script: null,
  lastTriggerAt: null,
  lastTriggerReason: null,
  lastError: null,
  lastOkDay: null,
};

function berlinParts(d = new Date()) {
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat("en-GB", {
      timeZone: DAILY_CRON_TZ,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      hour12: false,
    })
      .formatToParts(d)
      .filter((p) => p.type !== "literal")
      .map((p) => [p.type, p.value])
  );
  return {
    day: `${parts.year}${parts.month}${parts.day}`,
    hour: Number(parts.hour === "24" ? "0" : parts.hour),
  };
}

function resolveDailyScript() {
  const override = process.env.DAILY_SCRIPT_PATH;
  if (override && fs.existsSync(override)) return override;
  for (const dir of etlDirCandidates()) {
    const p = path.join(dir, "daily.sh");
    if (fs.existsSync(p)) return p;
  }
  return null;
}

function dailyOkPath(script, day) {
  return path.join(path.dirname(script), "logs", `daily_ok_${day}`);
}

function todayAlreadyOk(script) {
  const { day } = berlinParts();
  const ok = dailyOkPath(script, day);
  if (fs.existsSync(ok)) {
    dailyCronStatus.lastOkDay = day;
    return true;
  }
  return false;
}

function triggerDailyUpdate(reason) {
  const script = resolveDailyScript();
  dailyCronStatus.script = script;
  if (!script) {
    dailyCronStatus.lastError = "daily.sh not found";
    console.error("[enerlyzer] daily update: daily.sh not found in", etlDirCandidates());
    return false;
  }
  if (todayAlreadyOk(script)) {
    console.log(`[enerlyzer] daily update skipped (${reason}): already OK for ${berlinParts().day}`);
    return false;
  }

  dailyCronStatus.lastTriggerAt = new Date().toISOString();
  dailyCronStatus.lastTriggerReason = reason;
  dailyCronStatus.lastError = null;
  console.log(`[enerlyzer] daily update starting (${reason}): ${script}`);

  // setsid: child survives frequent Hostinger lsnode recycles during long imports.
  const child = spawn("/usr/bin/setsid", ["/bin/bash", script], {
    cwd: path.dirname(script),
    detached: true,
    stdio: "ignore",
    env: { ...process.env, TZ: DAILY_CRON_TZ },
  });
  child.unref();
  child.on("error", (err) => {
    // Fallback without setsid (unlikely missing on Hostinger)
    console.warn("[enerlyzer] setsid spawn failed, retrying with bash:", err.message);
    const fallback = spawn("/bin/bash", [script], {
      cwd: path.dirname(script),
      detached: true,
      stdio: "ignore",
      env: { ...process.env, TZ: DAILY_CRON_TZ },
    });
    fallback.unref();
    fallback.on("error", (e2) => {
      dailyCronStatus.lastError = String(e2.message || e2);
      console.error("[enerlyzer] daily update spawn failed:", e2.message);
    });
  });
  return true;
}

function maybeCatchUp(reason) {
  const script = resolveDailyScript();
  if (!script) return false;
  const { hour } = berlinParts();
  if (hour < 6) return false;
  if (todayAlreadyOk(script)) return false;
  return triggerDailyUpdate(reason);
}

function startDailyCron() {
  if (String(process.env.DAILY_CRON_DISABLE || "").match(/^(1|true|yes)$/i)) {
    console.log("[enerlyzer] daily cron disabled via DAILY_CRON_DISABLE");
    return;
  }
  if (!cron.validate(DAILY_CRON_EXPR) || !cron.validate(DAILY_CATCHUP_EXPR)) {
    console.error("[enerlyzer] invalid DAILY_CRON_EXPR / DAILY_CATCHUP_EXPR");
    return;
  }
  const script = resolveDailyScript();
  dailyCronStatus.script = script;
  dailyCronStatus.enabled = true;

  cron.schedule(DAILY_CRON_EXPR, () => triggerDailyUpdate("cron-0600"), {
    timezone: DAILY_CRON_TZ,
  });
  cron.schedule(DAILY_CATCHUP_EXPR, () => maybeCatchUp("catchup"), {
    timezone: DAILY_CRON_TZ,
  });

  console.log(
    `[enerlyzer] daily cron: "${DAILY_CRON_EXPR}" + catch-up "${DAILY_CATCHUP_EXPR}" tz=${DAILY_CRON_TZ}` +
      (script ? ` script=${script}` : " (WARNING: daily.sh not found yet)")
  );

  // If we boot after 06:00 and today's run is missing, start immediately.
  setTimeout(() => maybeCatchUp("startup"), 5_000);
}

app.get("/api/cron/status", (_req, res) => {
  const script = resolveDailyScript();
  const { day, hour } = berlinParts();
  const okPath = script ? dailyOkPath(script, day) : null;
  const ok = Boolean(okPath && fs.existsSync(okPath));
  if (ok) dailyCronStatus.lastOkDay = day;
  res.json({
    ...dailyCronStatus,
    script,
    scriptExists: Boolean(script && fs.existsSync(script)),
    berlinDay: day,
    berlinHour: hour,
    todayOk: ok,
    todayOkPath: okPath,
  });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`enerlyzer (Express) listening on :${PORT}`);
  startDailyCron();
});

