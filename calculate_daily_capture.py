"""
Calculate daily capture metrics - Adapted from user's working DuckDB query
Processes one country + one month at a time
"""

import os
import mysql.connector

DB_CONFIG = {
    'host': os.environ['DB_HOST'],
    'port': int(os.getenv('DB_PORT', '3306')),
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'database': os.getenv('DB_NAME', 'energy_market'),
    'use_pure': True,
    'connection_timeout': 600,
}

def process_country_month(cursor, conn, area_code, area_display_name, month):
    """Process one country + one month, return rows updated"""
    
    query = """
        WITH prices_raw AS (
            SELECT
                AreaCode,
                `DateTime(UTC)`,
                ResolutionCode,
                `Price[Currency/MWh]`,
                SUM(CASE WHEN ResolutionCode = 'PT60M' THEN 1 ELSE 0 END)
                    OVER (PARTITION BY AreaCode, `DateTime(UTC)`) AS cnt_60m_same_ts
            FROM energy_prices
            WHERE
                ContractType = 'Day-ahead'
                AND (`Sequence` IS NULL OR `Sequence` NOT IN ('2','3'))
                AND AreaCode = %s
                AND source_month = %s
        ),
        prices_dedup AS (
            SELECT *
            FROM prices_raw
            WHERE
                ResolutionCode = 'PT60M'
                OR (ResolutionCode = 'PT15M' AND cnt_60m_same_ts = 0)
        ),
        neg_hours AS (
            SELECT
                DATE(`DateTime(UTC)`) AS date_utc,
                SUM(
                    CASE
                        WHEN ResolutionCode = 'PT60M' AND `Price[Currency/MWh]` < 0 THEN 1.0
                        WHEN ResolutionCode = 'PT15M' AND `Price[Currency/MWh]` < 0 THEN 0.25
                        ELSE 0.0
                    END
                ) AS neg_hours_total
            FROM prices_dedup
            GROUP BY date_utc
        ),
        avg_price AS (
            SELECT
                DATE(`DateTime(UTC)`) AS date_utc,
                AVG(`Price[Currency/MWh]`) AS avg_price_eur_per_mwh
            FROM prices_dedup
            GROUP BY date_utc
        ),
        solar_raw AS (
            SELECT
                AreaCode,
                `DateTime(UTC)` AS datetime_utc,
                ActualGenerationOutput AS gen_mw
            FROM generation_per_type
            WHERE
                AreaCode = %s
                AND source_month = %s
                AND ProductionType = 'Solar'
                AND ActualGenerationOutput > 0
        ),
        joined AS (
            SELECT
                DATE(ep.`DateTime(UTC)`) AS date_utc,
                CASE
                    WHEN ep.ResolutionCode = 'PT15M' THEN 0.25
                    WHEN ep.ResolutionCode = 'PT60M' THEN 1.0
                    ELSE 1.0
                END AS interval_hours,
                ep.`Price[Currency/MWh]` AS price_raw,
                sr.gen_mw
            FROM prices_dedup ep
            JOIN solar_raw sr
                ON ep.AreaCode = sr.AreaCode
                AND ep.`DateTime(UTC)` = sr.datetime_utc
        ),
        solar_energy AS (
            SELECT
                date_utc,
                SUM(gen_mw * interval_hours) AS solar_mwh_total,
                SUM(
                    CASE WHEN price_raw < 0 THEN gen_mw * interval_hours ELSE 0 END
                ) AS solar_mwh_at_neg_price
            FROM joined
            GROUP BY date_utc
        ),
        capture AS (
            SELECT
                date_utc,
                SUM(gen_mw * interval_hours * price_raw)
                    / NULLIF(SUM(gen_mw * interval_hours), 0) AS capture_eur_per_mwh
            FROM joined
            GROUP BY date_utc
        ),
        capture_floor0 AS (
            SELECT
                date_utc,
                SUM(gen_mw * interval_hours * CASE WHEN price_raw < 0 THEN 0 ELSE price_raw END)
                    / NULLIF(SUM(gen_mw * interval_hours), 0) AS capture_price_floor0
            FROM joined
            GROUP BY date_utc
        ),
        daily_metrics AS (
            SELECT
                a.date_utc,
                DAY(a.date_utc) AS day_num,
                COALESCE(n.neg_hours_total, 0) AS neg_hours,
                ROUND(a.avg_price_eur_per_mwh, 2) AS avg_market_price,
                ROUND(c.capture_eur_per_mwh, 2) AS capture_price,
                ROUND(cf.capture_price_floor0, 2) AS capture_price_floor0,
                ROUND(100.0 * c.capture_eur_per_mwh / NULLIF(a.avg_price_eur_per_mwh, 0), 2) AS capture_rate,
                ROUND(100.0 * s.solar_mwh_at_neg_price / NULLIF(s.solar_mwh_total, 0), 2) AS solar_at_neg_price_pct
            FROM avg_price a
            LEFT JOIN neg_hours n ON a.date_utc = n.date_utc
            LEFT JOIN capture c ON a.date_utc = c.date_utc
            LEFT JOIN solar_energy s ON a.date_utc = s.date_utc
            LEFT JOIN capture_floor0 cf ON a.date_utc = cf.date_utc
        )
        UPDATE summary_daily sd
        JOIN daily_metrics dm ON sd.day = dm.day_num
        SET 
            sd.neg_hours = dm.neg_hours,
            sd.avg_market_price = dm.avg_market_price,
            sd.capture_price = COALESCE(dm.capture_price, 0),
            sd.capture_price_floor0 = COALESCE(dm.capture_price_floor0, 0),
            sd.capture_rate = COALESCE(dm.capture_rate, 0),
            sd.solar_at_neg_price_pct = COALESCE(dm.solar_at_neg_price_pct, 0)
        WHERE sd.country = %s AND sd.month = %s
    """
    
    cursor.execute(query, (area_code, month, area_code, month, area_display_name, month))
    conn.commit()
    return cursor.rowcount


def main():
    print("=" * 60, flush=True)
    print("Calculate Daily Metrics (Country by Country, Month by Month)", flush=True)
    print("=" * 60, flush=True)
    
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Get list of countries with their AreaCodes
    cursor.execute("""
        SELECT DISTINCT AreaDisplayName, AreaCode 
        FROM energy_prices 
        WHERE ContractType = 'Day-ahead'
        ORDER BY AreaDisplayName
    """)
    countries = cursor.fetchall()
    print(f"Found {len(countries)} countries", flush=True)
    
    # Get list of months
    cursor.execute("SELECT DISTINCT source_month FROM energy_prices ORDER BY source_month")
    months = [row[0] for row in cursor.fetchall()]
    print(f"Months: {months}", flush=True)
    
    total_updated = 0
    total_tasks = len(countries) * len(months)
    task_num = 0
    
    skipped = 0
    for area_display_name, area_code in countries:
        for month in months:
            task_num += 1
            
            # Check if already processed (capture_price > 0)
            cursor.execute("""
                SELECT COUNT(*) FROM summary_daily 
                WHERE country = %s AND month = %s AND capture_price > 0
            """, (area_display_name, month))
            already_done = cursor.fetchone()[0]
            
            if already_done > 0:
                skipped += 1
                print(f"[{task_num}/{total_tasks}] {area_display_name}, Month {month}... SKIPPED (already done)", flush=True)
                continue
            
            print(f"[{task_num}/{total_tasks}] {area_display_name}, Month {month}...", end=" ", flush=True)
            
            # Retry logic for connection drops
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    # Reconnect if needed
                    if not conn.is_connected():
                        print("(reconnecting...)", end=" ", flush=True)
                        conn = mysql.connector.connect(**DB_CONFIG)
                        cursor = conn.cursor()
                    
                    updated = process_country_month(cursor, conn, area_code, area_display_name, month)
                    total_updated += updated
                    print(f"{updated} rows", flush=True)
                    break  # Success, exit retry loop
                    
                except Exception as e:
                    if attempt < max_retries - 1:
                        import time
                        wait_time = 30 * (attempt + 1)  # 30s, 60s, 90s...
                        print(f"ERROR: {e}", flush=True)
                        print(f"        Retrying in {wait_time}s (attempt {attempt+2}/{max_retries})...", flush=True)
                        time.sleep(wait_time)
                        # Force reconnect
                        try:
                            conn.close()
                        except:
                            pass
                        conn = mysql.connector.connect(**DB_CONFIG)
                        cursor = conn.cursor()
                    else:
                        print(f"FAILED after {max_retries} attempts: {e}", flush=True)
    
    print(f"\nSkipped {skipped} already-processed country/months", flush=True)
    
    # Verification
    print("\n" + "=" * 60, flush=True)
    print("Verification (DE-LU, April):", flush=True)
    cursor.execute("""
        SELECT day, neg_hours, avg_market_price, capture_price, capture_rate, solar_at_neg_price_pct
        FROM summary_daily 
        WHERE country = 'DE-LU' AND month = 4 
        ORDER BY day LIMIT 10
    """)
    for row in cursor.fetchall():
        print(f"  Day {row[0]}: neg={row[1]}, avg=€{row[2]}, cap=€{row[3]}, rate={row[4]}%, solar_neg={row[5]}%", flush=True)
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 60, flush=True)
    print(f"✅ Done! Total rows updated: {total_updated}", flush=True)

if __name__ == "__main__":
    main()
