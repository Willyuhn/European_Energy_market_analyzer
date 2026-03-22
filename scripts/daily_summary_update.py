#!/usr/bin/env python3
"""
Daily Summary Update Script
===========================
Runs 3-phase summary table update:
  Phase 1: INSERT IGNORE basic daily rows (neg_hours, avg_market_price)
  Phase 2: UPDATE capture metrics via JOIN (capture_price, capture_price_floor0, solar_at_neg_price_pct, capture_rate)
  Phase 3: Refresh summary_monthly and summary_yearly from summary_daily

Environment Variables:
  DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME - Database credentials
  TARGET_YEAR - Override year (default: current UTC year)

Designed for low-memory database (db-f1-micro). Uses INSERT IGNORE for idempotency.
"""
import os
import sys
from datetime import datetime
import mysql.connector

# =============================================================================
# Configuration
# =============================================================================
DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "port": int(os.environ.get("DB_PORT", "3306")),
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "database": os.environ.get("DB_NAME", "energy_market"),
    "use_pure": True,
    "connection_timeout": 300,
    "autocommit": True,
}

# Target year: use TARGET_YEAR env var or current UTC year
TARGET_YEAR = int(os.environ.get("TARGET_YEAR", datetime.utcnow().year))


def log(msg):
    """Print with timestamp and flush"""
    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def execute_sql(cursor, sql, description):
    """Execute SQL and log row count"""
    log(f"  Executing: {description}...")
    cursor.execute(sql)
    row_count = cursor.rowcount
    log(f"  ✓ {description}: {row_count} rows affected")
    return row_count


def main():
    log("=" * 60)
    log("DAILY SUMMARY UPDATE")
    log(f"Target Year: {TARGET_YEAR}")
    log("=" * 60)

    # Connect to database
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        log("✓ Connected to database")
    except Exception as e:
        log(f"✗ Database connection failed: {e}")
        sys.exit(1)

    try:
        # =====================================================================
        # PHASE 1: Insert basic daily rows (INSERT IGNORE - no duplicates)
        # =====================================================================
        log("")
        log("PHASE 1: Insert basic daily rows (neg_hours, avg_market_price)")
        log("-" * 40)

        phase1_sql = f"""
        INSERT IGNORE INTO summary_daily (year, country, month, day, neg_hours, avg_market_price)
        SELECT 
            {TARGET_YEAR}, 
            AreaDisplayName, 
            MONTH(`DateTime(UTC)`), 
            DAY(`DateTime(UTC)`),
            SUM(CASE WHEN `Price[Currency/MWh]` < 0 THEN 0.25 ELSE 0 END),
            ROUND(AVG(`Price[Currency/MWh]`), 2)
        FROM energy_prices
        WHERE YEAR(`DateTime(UTC)`) = {TARGET_YEAR}
          AND ContractType = 'Day-ahead'
          AND (`Sequence` IS NULL OR `Sequence` NOT IN ('2', '3'))
        GROUP BY AreaDisplayName, MONTH(`DateTime(UTC)`), DAY(`DateTime(UTC)`)
        """
        
        rows_inserted = execute_sql(cursor, phase1_sql, "INSERT IGNORE daily rows")
        log(f"  Phase 1 complete: {rows_inserted} new rows inserted")

        # =====================================================================
        # PHASE 2: Update capture metrics (JOIN with generation_per_type)
        # =====================================================================
        log("")
        log("PHASE 2: Update capture metrics (capture_price, capture_price_floor0, solar_at_neg_price_pct)")
        log("-" * 40)

        phase2a_sql = f"""
        UPDATE summary_daily sd
        JOIN (
            SELECT 
                ep.AreaDisplayName AS country,
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
            WHERE YEAR(ep.`DateTime(UTC)`) = {TARGET_YEAR}
              AND ep.ContractType = 'Day-ahead'
              AND gp.ProductionType = 'Solar' 
              AND gp.ActualGenerationOutput > 0
            GROUP BY ep.AreaDisplayName, MONTH(ep.`DateTime(UTC)`), DAY(ep.`DateTime(UTC)`)
        ) cap ON sd.country = cap.country AND sd.month = cap.month AND sd.day = cap.day
        SET sd.capture_price = cap.capture_price,
            sd.capture_price_floor0 = cap.capture_price_floor0,
            sd.solar_at_neg_price_pct = cap.solar_pct
        WHERE sd.year = {TARGET_YEAR}
        """
        
        rows_updated = execute_sql(cursor, phase2a_sql, "UPDATE capture metrics")

        phase2b_sql = f"""
        UPDATE summary_daily 
        SET capture_rate = ROUND(100.0 * capture_price / NULLIF(avg_market_price, 0), 2)
        WHERE year = {TARGET_YEAR} AND avg_market_price > 0
        """
        
        execute_sql(cursor, phase2b_sql, "UPDATE capture_rate")
        log(f"  Phase 2 complete: {rows_updated} rows updated with capture metrics")

        # =====================================================================
        # PHASE 3: Refresh summary_monthly and summary_yearly
        # =====================================================================
        log("")
        log("PHASE 3: Refresh summary_monthly and summary_yearly")
        log("-" * 40)

        # Monthly
        execute_sql(cursor, f"DELETE FROM summary_monthly WHERE year = {TARGET_YEAR}", 
                   "DELETE old monthly rows")

        phase3_monthly_sql = f"""
        INSERT INTO summary_monthly (year, country, month, neg_hours, avg_market_price, 
                                     capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct)
        SELECT 
            {TARGET_YEAR}, country, month,
            SUM(neg_hours),
            ROUND(AVG(avg_market_price), 2),
            ROUND(AVG(capture_price), 2),
            ROUND(AVG(capture_price_floor0), 2),
            ROUND(AVG(capture_rate), 2),
            ROUND(AVG(solar_at_neg_price_pct), 2)
        FROM summary_daily
        WHERE year = {TARGET_YEAR}
        GROUP BY country, month
        """
        
        monthly_rows = execute_sql(cursor, phase3_monthly_sql, "INSERT monthly rows")

        # Yearly
        execute_sql(cursor, f"DELETE FROM summary_yearly WHERE year = {TARGET_YEAR}",
                   "DELETE old yearly rows")

        phase3_yearly_sql = f"""
        INSERT INTO summary_yearly (year, country, total_neg_hours, avg_market_price, 
                                    capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct)
        SELECT 
            {TARGET_YEAR}, country,
            SUM(neg_hours),
            ROUND(AVG(avg_market_price), 2),
            ROUND(AVG(capture_price), 2),
            ROUND(AVG(capture_price_floor0), 2),
            ROUND(AVG(capture_rate), 2),
            ROUND(AVG(solar_at_neg_price_pct), 2)
        FROM summary_monthly
        WHERE year = {TARGET_YEAR}
        GROUP BY country
        """
        
        yearly_rows = execute_sql(cursor, phase3_yearly_sql, "INSERT yearly rows")
        log(f"  Phase 3 complete: {monthly_rows} monthly, {yearly_rows} yearly rows")

        # =====================================================================
        # DONE
        # =====================================================================
        log("")
        log("=" * 60)
        log("✓ DAILY SUMMARY UPDATE COMPLETE")
        log(f"  Year: {TARGET_YEAR}")
        log("=" * 60)

        # Quick verification
        cursor.execute(f"SELECT COUNT(*) FROM summary_daily WHERE year = {TARGET_YEAR}")
        daily_count = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM summary_monthly WHERE year = {TARGET_YEAR}")
        monthly_count = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM summary_yearly WHERE year = {TARGET_YEAR}")
        yearly_count = cursor.fetchone()[0]
        
        log("")
        log(f"Final counts for {TARGET_YEAR}:")
        log(f"  summary_daily:   {daily_count} rows")
        log(f"  summary_monthly: {monthly_count} rows")
        log(f"  summary_yearly:  {yearly_count} rows")

    except Exception as e:
        log(f"✗ ERROR: {e}")
        cursor.close()
        conn.close()
        sys.exit(1)

    cursor.close()
    conn.close()
    log("")
    log("✓ Done!")


if __name__ == "__main__":
    main()
