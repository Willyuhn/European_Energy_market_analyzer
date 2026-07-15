#!/usr/bin/env python3
"""
Build/refresh all summary tables consumed by the dashboard (server.js):
  summary_daily, summary_monthly, summary_yearly, summary_total

Matches the schema created by scripts/etl/entsoe_import.py --bootstrap-schema
(no legacy `source_month` column). Data written by entsoe_import is always
normalized to a full 15-minute grid with ResolutionCode='PT15M', so a negative
15-minute interval counts as 0.25 h.

Idempotent: for every year found in energy_prices the target rows are deleted
and recomputed, so re-runs never duplicate. summary_total is rebuilt from
summary_yearly across all years.

Environment (repo-root .env is loaded automatically if python-dotenv is present):
  DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
  SUMMARY_YEARS  optional, comma-separated (e.g. "2025,2026"); default: all
                 distinct years present in energy_prices.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass

import mysql.connector

DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "port": int(os.environ.get("DB_PORT", "3306")),
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "database": os.environ.get("DB_NAME", "energy_market"),
    "use_pure": True,
    "connection_timeout": 600,
    "autocommit": False,
}


def log(msg: str) -> None:
    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def run(cursor, sql: str, params=None, desc: str = "") -> int:
    cursor.execute(sql, params or ())
    n = cursor.rowcount
    if desc:
        log(f"  ✓ {desc}: {n} rows")
    return n


def discover_years(cursor) -> list[int]:
    env = (os.environ.get("SUMMARY_YEARS") or "").strip()
    if env:
        return [int(y) for y in env.split(",") if y.strip()]
    cursor.execute("SELECT DISTINCT YEAR(`DateTime(UTC)`) FROM energy_prices ORDER BY 1")
    return [int(r[0]) for r in cursor.fetchall() if r[0] is not None]


def build_year(cursor, year: int) -> None:
    log(f"Year {year}: rebuilding summary_daily / summary_monthly / summary_yearly")

    # ---- summary_daily: neg_hours + avg_market_price (prices only) ----
    run(cursor, "DELETE FROM summary_daily WHERE year = %s", (year,), "clear daily")
    run(
        cursor,
        """
        INSERT INTO summary_daily (year, country, month, day, neg_hours, avg_market_price)
        SELECT %s, AreaDisplayName, MONTH(`DateTime(UTC)`), DAY(`DateTime(UTC)`),
               SUM(CASE WHEN `Price[Currency/MWh]` < 0 THEN 0.25 ELSE 0 END),
               ROUND(AVG(`Price[Currency/MWh]`), 2)
        FROM energy_prices
        WHERE YEAR(`DateTime(UTC)`) = %s
          AND ContractType = 'Day-ahead'
          AND (`Sequence` IS NULL OR `Sequence` NOT IN ('2', '3'))
        GROUP BY AreaDisplayName, MONTH(`DateTime(UTC)`), DAY(`DateTime(UTC)`)
        """,
        (year, year),
        "insert daily neg_hours/avg_price",
    )

    # ---- summary_daily: capture metrics (join with solar generation) ----
    run(
        cursor,
        """
        UPDATE summary_daily sd
        JOIN (
            SELECT ep.AreaDisplayName AS country,
                   MONTH(ep.`DateTime(UTC)`) AS month,
                   DAY(ep.`DateTime(UTC)`) AS day,
                   ROUND(SUM(gp.ActualGenerationOutput * 0.25 * ep.`Price[Currency/MWh]`) /
                         NULLIF(SUM(gp.ActualGenerationOutput * 0.25), 0), 2) AS capture_price,
                   ROUND(SUM(gp.ActualGenerationOutput * 0.25 *
                         CASE WHEN ep.`Price[Currency/MWh]` < 0 THEN 0 ELSE ep.`Price[Currency/MWh]` END) /
                         NULLIF(SUM(gp.ActualGenerationOutput * 0.25), 0), 2) AS capture_price_floor0,
                   ROUND(100.0 * SUM(CASE WHEN ep.`Price[Currency/MWh]` < 0 THEN gp.ActualGenerationOutput * 0.25 ELSE 0 END) /
                         NULLIF(SUM(gp.ActualGenerationOutput * 0.25), 0), 2) AS solar_pct
            FROM energy_prices ep
            JOIN generation_per_type gp
              ON ep.AreaCode = gp.AreaCode AND ep.`DateTime(UTC)` = gp.`DateTime(UTC)`
            WHERE YEAR(ep.`DateTime(UTC)`) = %s
              AND ep.ContractType = 'Day-ahead'
              AND gp.ProductionType = 'Solar'
              AND gp.ActualGenerationOutput > 0
            GROUP BY ep.AreaDisplayName, MONTH(ep.`DateTime(UTC)`), DAY(ep.`DateTime(UTC)`)
        ) cap ON sd.country = cap.country AND sd.month = cap.month AND sd.day = cap.day
        SET sd.capture_price = cap.capture_price,
            sd.capture_price_floor0 = cap.capture_price_floor0,
            sd.solar_at_neg_price_pct = cap.solar_pct
        WHERE sd.year = %s
        """,
        (year, year),
        "update daily capture metrics",
    )
    run(
        cursor,
        """
        UPDATE summary_daily
        SET capture_rate = ROUND(100.0 * capture_price / NULLIF(avg_market_price, 0), 2)
        WHERE year = %s AND avg_market_price > 0
        """,
        (year,),
        "update daily capture_rate",
    )

    # ---- summary_monthly: recompute from raw tables (not AVG of daily values) ----
    # Marktwert Solar over a month must be SUM(P×E)/SUM(E) for the whole month,
    # not the average of daily capture prices (equal weight per day, wrong).
    run(cursor, "DELETE FROM summary_monthly WHERE year = %s", (year,), "clear monthly")
    run(
        cursor,
        """
        INSERT INTO summary_monthly (year, country, month, neg_hours, avg_market_price)
        SELECT %s, AreaDisplayName, MONTH(`DateTime(UTC)`),
               SUM(CASE WHEN `Price[Currency/MWh]` < 0 THEN 0.25 ELSE 0 END),
               ROUND(AVG(`Price[Currency/MWh]`), 2)
        FROM energy_prices
        WHERE YEAR(`DateTime(UTC)`) = %s
          AND ContractType = 'Day-ahead'
          AND (`Sequence` IS NULL OR `Sequence` NOT IN ('2', '3'))
        GROUP BY AreaDisplayName, MONTH(`DateTime(UTC)`)
        """,
        (year, year),
        "insert monthly neg_hours/avg_price",
    )
    run(
        cursor,
        """
        UPDATE summary_monthly sm
        JOIN (
            SELECT ep.AreaDisplayName AS country,
                   MONTH(ep.`DateTime(UTC)`) AS month,
                   ROUND(SUM(gp.ActualGenerationOutput * 0.25 * ep.`Price[Currency/MWh]`) /
                         NULLIF(SUM(gp.ActualGenerationOutput * 0.25), 0), 2) AS capture_price,
                   ROUND(SUM(gp.ActualGenerationOutput * 0.25 *
                         CASE WHEN ep.`Price[Currency/MWh]` < 0 THEN 0 ELSE ep.`Price[Currency/MWh]` END) /
                         NULLIF(SUM(gp.ActualGenerationOutput * 0.25), 0), 2) AS capture_price_floor0,
                   ROUND(100.0 * SUM(CASE WHEN ep.`Price[Currency/MWh]` < 0
                         THEN gp.ActualGenerationOutput * 0.25 ELSE 0 END) /
                         NULLIF(SUM(gp.ActualGenerationOutput * 0.25), 0), 2) AS solar_pct
            FROM energy_prices ep
            JOIN generation_per_type gp
              ON ep.AreaCode = gp.AreaCode AND ep.`DateTime(UTC)` = gp.`DateTime(UTC)`
            WHERE YEAR(ep.`DateTime(UTC)`) = %s
              AND ep.ContractType = 'Day-ahead'
              AND gp.ProductionType = 'Solar'
              AND gp.ActualGenerationOutput > 0
            GROUP BY ep.AreaDisplayName, MONTH(ep.`DateTime(UTC)`)
        ) cap ON sm.country = cap.country AND sm.month = cap.month
        SET sm.capture_price = cap.capture_price,
            sm.capture_price_floor0 = cap.capture_price_floor0,
            sm.solar_at_neg_price_pct = cap.solar_pct
        WHERE sm.year = %s
        """,
        (year, year),
        "update monthly capture metrics",
    )
    run(
        cursor,
        """
        UPDATE summary_monthly
        SET capture_rate = ROUND(100.0 * capture_price / NULLIF(avg_market_price, 0), 2)
        WHERE year = %s AND avg_market_price > 0
        """,
        (year,),
        "update monthly capture_rate",
    )

    # ---- summary_yearly: recompute from raw tables ----
    run(cursor, "DELETE FROM summary_yearly WHERE year = %s", (year,), "clear yearly")
    run(
        cursor,
        """
        INSERT INTO summary_yearly (year, country, total_neg_hours, avg_market_price)
        SELECT %s, AreaDisplayName,
               SUM(CASE WHEN `Price[Currency/MWh]` < 0 THEN 0.25 ELSE 0 END),
               ROUND(AVG(`Price[Currency/MWh]`), 2)
        FROM energy_prices
        WHERE YEAR(`DateTime(UTC)`) = %s
          AND ContractType = 'Day-ahead'
          AND (`Sequence` IS NULL OR `Sequence` NOT IN ('2', '3'))
        GROUP BY AreaDisplayName
        """,
        (year, year),
        "insert yearly neg_hours/avg_price",
    )
    run(
        cursor,
        """
        UPDATE summary_yearly sy
        JOIN (
            SELECT ep.AreaDisplayName AS country,
                   ROUND(SUM(gp.ActualGenerationOutput * 0.25 * ep.`Price[Currency/MWh]`) /
                         NULLIF(SUM(gp.ActualGenerationOutput * 0.25), 0), 2) AS capture_price,
                   ROUND(SUM(gp.ActualGenerationOutput * 0.25 *
                         CASE WHEN ep.`Price[Currency/MWh]` < 0 THEN 0 ELSE ep.`Price[Currency/MWh]` END) /
                         NULLIF(SUM(gp.ActualGenerationOutput * 0.25), 0), 2) AS capture_price_floor0,
                   ROUND(100.0 * SUM(CASE WHEN ep.`Price[Currency/MWh]` < 0
                         THEN gp.ActualGenerationOutput * 0.25 ELSE 0 END) /
                         NULLIF(SUM(gp.ActualGenerationOutput * 0.25), 0), 2) AS solar_pct
            FROM energy_prices ep
            JOIN generation_per_type gp
              ON ep.AreaCode = gp.AreaCode AND ep.`DateTime(UTC)` = gp.`DateTime(UTC)`
            WHERE YEAR(ep.`DateTime(UTC)`) = %s
              AND ep.ContractType = 'Day-ahead'
              AND gp.ProductionType = 'Solar'
              AND gp.ActualGenerationOutput > 0
            GROUP BY ep.AreaDisplayName
        ) cap ON sy.country = cap.country
        SET sy.capture_price = cap.capture_price,
            sy.capture_price_floor0 = cap.capture_price_floor0,
            sy.solar_at_neg_price_pct = cap.solar_pct
        WHERE sy.year = %s
        """,
        (year, year),
        "update yearly capture metrics",
    )
    run(
        cursor,
        """
        UPDATE summary_yearly
        SET capture_rate = ROUND(100.0 * capture_price / NULLIF(avg_market_price, 0), 2)
        WHERE year = %s AND avg_market_price > 0
        """,
        (year,),
        "update yearly capture_rate",
    )


def build_total(cursor) -> None:
    log("summary_total: rebuilding from summary_yearly (all years)")
    run(cursor, "DELETE FROM summary_total", desc="clear total")
    run(
        cursor,
        """
        INSERT INTO summary_total (id, total_neg_hours, avg_market_price,
                                   capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct)
        SELECT 1,
               SUM(total_neg_hours),
               ROUND(AVG(avg_market_price), 2),
               ROUND(AVG(capture_price), 2),
               ROUND(AVG(capture_price_floor0), 2),
               ROUND(AVG(capture_rate), 2),
               ROUND(AVG(solar_at_neg_price_pct), 2)
        FROM summary_yearly
        """,
        desc="insert total",
    )


def main() -> None:
    for v in ("DB_HOST", "DB_USER", "DB_PASSWORD"):
        if v not in os.environ:
            log(f"✗ Missing {v}")
            sys.exit(1)

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        years = discover_years(cursor)
        if not years:
            log("✗ No data in energy_prices — nothing to summarize.")
            sys.exit(1)
        log("=" * 60)
        log(f"Building summaries for years: {years}")
        log("=" * 60)
        for y in years:
            build_year(cursor, y)
            conn.commit()
        build_total(cursor)
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM summary_daily")
        d = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM summary_monthly")
        m = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM summary_yearly")
        yv = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM summary_total")
        t = cursor.fetchone()[0]
        log("=" * 60)
        log(f"✓ Done. summary_daily={d} summary_monthly={m} summary_yearly={yv} summary_total={t}")
        log("=" * 60)
    except Exception as e:
        conn.rollback()
        log(f"✗ ERROR: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
