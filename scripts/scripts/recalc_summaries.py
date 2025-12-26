"""
Recalculate summary tables from raw ENTSO-E data.

- Rebuilds summary_daily for the last N days (default: 5)
- Rebuilds monthly/yearly/total summaries from summary_daily
- Designed to run as a Cloud Run Job
"""

import os
import sys
from datetime import datetime, timedelta
import mysql.connector

# =============================================================================
# CONFIG
# =============================================================================

DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "port": int(os.environ.get("DB_PORT", "3306")),
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "database": os.environ.get("DB_NAME", "energy_market"),
    "use_pure": True,
    "connection_timeout": 600,
}

WINDOW_DAYS = int(os.environ.get("SUMMARY_WINDOW_DAYS", "5"))

# =============================================================================
# HELPERS
# =============================================================================

def get_conn():
    return mysql.connector.connect(**DB_CONFIG)

# =============================================================================
# SUMMARY DAILY
# =============================================================================

def rebuild_summary_daily(conn, start_dt, end_dt):
    print(f"\n🔄 Rebuilding summary_daily ({start_dt.date()} → {end_dt.date()})", flush=True)
    cur = conn.cursor()

    # --- delete affected days (rolling window)
    cur.execute("""
        DELETE FROM summary_daily
        WHERE STR_TO_DATE(
                CONCAT(YEAR(CURRENT_DATE), '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),
                '%Y-%m-%d'
              ) >= CURRENT_DATE - INTERVAL %s DAY
    """, (WINDOW_DAYS,))
    conn.commit()

    # --- insert base metrics (neg hours + avg price)
    cur.execute("""
        INSERT INTO summary_daily (country, month, day, neg_hours, avg_market_price)
        WITH prices_raw AS (
            SELECT
                AreaCode,
                AreaDisplayName,
                `DateTime(UTC)`,
                ResolutionCode,
                `Price[Currency/MWh]`,
                source_month,
                DAY(`DateTime(UTC)`) AS day_num,
                SUM(CASE WHEN ResolutionCode = 'PT60M' THEN 1 ELSE 0 END)
                    OVER (PARTITION BY AreaCode, `DateTime(UTC)`) AS cnt_60m_same_ts
            FROM energy_prices
            WHERE ContractType = 'Day-ahead'
              AND (`Sequence` IS NULL OR `Sequence` NOT IN ('2','3'))
              AND `DateTime(UTC)` >= %s
        ),
        prices_dedup AS (
            SELECT *
            FROM prices_raw
            WHERE ResolutionCode = 'PT60M'
               OR (ResolutionCode = 'PT15M' AND cnt_60m_same_ts = 0)
        )
        SELECT
            AreaDisplayName AS country,
            source_month AS month,
            day_num AS day,
            SUM(
                CASE
                    WHEN ResolutionCode = 'PT60M' AND `Price[Currency/MWh]` < 0 THEN 1.0
                    WHEN ResolutionCode = 'PT15M' AND `Price[Currency/MWh]` < 0 THEN 0.25
                    ELSE 0.0
                END
            ) AS neg_hours,
            ROUND(AVG(`Price[Currency/MWh]`), 2) AS avg_market_price
        FROM prices_dedup
        GROUP BY AreaDisplayName, source_month, day_num
    """, (start_dt,))
    conn.commit()
    print(f"   ✅ summary_daily base rows inserted: {cur.rowcount}", flush=True)

    # --- capture metrics
    cur.execute("DROP TABLE IF EXISTS temp_daily_capture")
    cur.execute("""
        CREATE TABLE temp_daily_capture AS
        WITH prices_raw AS (
            SELECT
                AreaCode,
                AreaDisplayName,
                `DateTime(UTC)`,
                ResolutionCode,
                `Price[Currency/MWh]`,
                source_month,
                DAY(`DateTime(UTC)`) AS day_num,
                SUM(CASE WHEN ResolutionCode = 'PT60M' THEN 1 ELSE 0 END)
                    OVER (PARTITION BY AreaCode, `DateTime(UTC)`) AS cnt_60m_same_ts
            FROM energy_prices
            WHERE ContractType = 'Day-ahead'
              AND (`Sequence` IS NULL OR `Sequence` NOT IN ('2','3'))
              AND `DateTime(UTC)` >= %s
        ),
        prices_dedup AS (
            SELECT *
            FROM prices_raw
            WHERE ResolutionCode = 'PT60M'
               OR (ResolutionCode = 'PT15M' AND cnt_60m_same_ts = 0)
        ),
        joined AS (
            SELECT
                ep.AreaDisplayName AS country,
                ep.source_month AS month,
                DAY(ep.`DateTime(UTC)`) AS day,
                CASE WHEN ep.ResolutionCode = 'PT15M' THEN 0.25 ELSE 1.0 END AS interval_hours,
                ep.`Price[Currency/MWh]` AS price_raw,
                gp.ActualGenerationOutput AS gen_mw
            FROM prices_dedup ep
            JOIN generation_per_type gp
              ON ep.AreaCode = gp.AreaCode
             AND ep.`DateTime(UTC)` = gp.`DateTime(UTC)`
             AND ep.source_month = gp.source_month
            WHERE gp.ProductionType = 'Solar'
              AND gp.ActualGenerationOutput > 0
        )
        SELECT
            country, month, day,
            ROUND(SUM(gen_mw * interval_hours * price_raw)
                  / NULLIF(SUM(gen_mw * interval_hours), 0), 2) AS capture_price,
            ROUND(SUM(gen_mw * interval_hours *
                      CASE WHEN price_raw < 0 THEN 0 ELSE price_raw END)
                  / NULLIF(SUM(gen_mw * interval_hours), 0), 2) AS capture_price_floor0,
            ROUND(100.0 * SUM(CASE WHEN price_raw < 0 THEN gen_mw * interval_hours ELSE 0 END)
                  / NULLIF(SUM(gen_mw * interval_hours), 0), 2) AS solar_at_neg_price_pct
        FROM joined
        GROUP BY country, month, day
    """, (start_dt,))
    conn.commit()

    cur.execute("""
        UPDATE summary_daily sd
        JOIN temp_daily_capture tc
          ON sd.country = tc.country
         AND sd.month   = tc.month
         AND sd.day     = tc.day
        SET
          sd.capture_price            = tc.capture_price,
          sd.capture_price_floor0     = tc.capture_price_floor0,
          sd.solar_at_neg_price_pct   = tc.solar_at_neg_price_pct
    """)
    conn.commit()

    cur.execute("""
        UPDATE summary_daily
        SET capture_rate = ROUND(100.0 * capture_price / NULLIF(avg_market_price, 0), 2)
        WHERE avg_market_price > 0
    """)
    conn.commit()

    cur.execute("DROP TABLE IF EXISTS temp_daily_capture")
    conn.commit()

    cur.close()
    print("   ✅ summary_daily updated", flush=True)

# =============================================================================
# ROLLUPS
# =============================================================================

def rebuild_rollups(conn):
    print("\n🔄 Rebuilding monthly / yearly / total summaries", flush=True)
    cur = conn.cursor()

    # monthly
    cur.execute("""
        DELETE FROM summary_monthly
        WHERE month IN (
            SELECT DISTINCT month FROM summary_daily
            WHERE STR_TO_DATE(
                CONCAT(YEAR(CURRENT_DATE), '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),
                '%Y-%m-%d'
            ) >= CURRENT_DATE - INTERVAL %s DAY
        )
    """, (WINDOW_DAYS,))
    conn.commit()

    cur.execute("""
        INSERT INTO summary_monthly
        (country, month, neg_hours, avg_market_price,
         capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct)
        SELECT
            country,
            month,
            SUM(neg_hours),
            ROUND(AVG(avg_market_price), 2),
            ROUND(AVG(NULLIF(capture_price, 0)), 2),
            ROUND(AVG(NULLIF(capture_price_floor0, 0)), 2),
            ROUND(AVG(NULLIF(capture_rate, 0)), 2),
            ROUND(AVG(NULLIF(solar_at_neg_price_pct, 0)), 2)
        FROM summary_daily
        GROUP BY country, month
    """)
    conn.commit()

    # yearly
    cur.execute("TRUNCATE TABLE summary_yearly")
    cur.execute("""
        INSERT INTO summary_yearly
        (country, total_neg_hours, avg_market_price,
         capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct)
        SELECT
            country,
            SUM(neg_hours),
            ROUND(AVG(avg_market_price), 2),
            ROUND(AVG(capture_price), 2),
            ROUND(AVG(capture_price_floor0), 2),
            ROUND(AVG(capture_rate), 2),
            ROUND(AVG(solar_at_neg_price_pct), 2)
        FROM summary_monthly
        GROUP BY country
    """)
    conn.commit()

    # total
    cur.execute("TRUNCATE TABLE summary_total")
    cur.execute("""
        INSERT INTO summary_total
        (id, total_neg_hours, avg_market_price,
         capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct)
        SELECT
            1,
            SUM(total_neg_hours),
            ROUND(AVG(avg_market_price), 2),
            ROUND(AVG(capture_price), 2),
            ROUND(AVG(capture_price_floor0), 2),
            ROUND(AVG(capture_rate), 2),
            ROUND(AVG(solar_at_neg_price_pct), 2)
        FROM summary_yearly
    """)
    conn.commit()

    cur.close()
    print("   ✅ rollups rebuilt", flush=True)

# =============================================================================
# MAIN
# =============================================================================

def main():
    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=WINDOW_DAYS)

    print("=" * 70)
    print("Summary recalculation job")
    print(f"Window: last {WINDOW_DAYS} days")
    print("=" * 70)

    conn = get_conn()
    try:
        rebuild_summary_daily(conn, start_dt, end_dt)
        rebuild_rollups(conn)
        print("\n✅ Summary recalculation complete", flush=True)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
