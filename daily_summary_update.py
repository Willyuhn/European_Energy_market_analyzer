"""
Daily Summary Update Script
- Only processes CURRENT YEAR and CURRENT MONTH
- Previous years and months are locked
- Runs after daily data fetch (6:15 AM)
"""
import os
from datetime import datetime
import mysql.connector

DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "port": int(os.environ.get("DB_PORT", "3306")),
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "database": os.environ.get("DB_NAME", "energy_market"),
    "use_pure": True,
    "connection_timeout": 600,
}

def main():
    now = datetime.utcnow()
    current_year = now.year
    current_month = now.month
    
    print("=" * 60, flush=True)
    print("Daily Summary Update", flush=True)
    print(f"Processing: {current_year}-{current_month:02d} only", flush=True)
    print("=" * 60, flush=True)
    
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("✅ Connected to database", flush=True)
    
    # =========================================================
    # STEP 1: Delete and recalculate summary_daily for current month only
    # =========================================================
    print(f"\n1. Recalculating summary_daily for {current_year}-{current_month:02d}...", flush=True)
    
    # Delete current month's daily data
    cursor.execute("""
        DELETE FROM summary_daily 
        WHERE year = %s AND month = %s
    """, (current_year, current_month))
    deleted = cursor.rowcount
    print(f"   Deleted {deleted} existing rows for {current_year}-{current_month:02d}", flush=True)
    
    # Create deduplicated prices for current month
    print("   Creating deduplicated price data...", flush=True)
    cursor.execute("DROP TABLE IF EXISTS temp_prices_current_month")
    cursor.execute("""
        CREATE TEMPORARY TABLE temp_prices_current_month AS
        SELECT * FROM (
            SELECT 
                id, AreaCode, AreaDisplayName, `DateTime(UTC)`, 
                `Price[Currency/MWh]`, ResolutionCode,
                ROW_NUMBER() OVER (
                    PARTITION BY AreaCode, `DateTime(UTC)` 
                    ORDER BY id DESC
                ) AS rn
            FROM energy_prices
            WHERE YEAR(`DateTime(UTC)`) = %s
              AND MONTH(`DateTime(UTC)`) = %s
              AND ContractType = 'Day-ahead'
              AND (`Sequence` IS NULL OR `Sequence` NOT IN ('2', '3'))
        ) ranked
        WHERE rn = 1
    """, (current_year, current_month))
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM temp_prices_current_month")
    print(f"   Deduplicated price records: {cursor.fetchone()[0]}", flush=True)
    
    # Insert daily neg_hours and avg_market_price
    print("   Calculating daily metrics...", flush=True)
    cursor.execute("""
        INSERT INTO summary_daily (year, country, month, day, neg_hours, avg_market_price)
        SELECT 
            %s,
            AreaDisplayName,
            %s,
            DAY(`DateTime(UTC)`),
            SUM(CASE WHEN `Price[Currency/MWh]` < 0 THEN 0.25 ELSE 0 END),
            ROUND(AVG(`Price[Currency/MWh]`), 2)
        FROM temp_prices_current_month
        GROUP BY AreaDisplayName, DAY(`DateTime(UTC)`)
    """, (current_year, current_month))
    inserted_daily = cursor.rowcount
    conn.commit()
    print(f"   Inserted {inserted_daily} daily rows", flush=True)
    
    # Calculate daily capture metrics
    print("   Calculating daily capture metrics...", flush=True)
    cursor.execute("DROP TABLE IF EXISTS temp_daily_capture")
    cursor.execute("""
        CREATE TEMPORARY TABLE temp_daily_capture AS
        SELECT
            ep.AreaDisplayName AS country,
            DAY(ep.`DateTime(UTC)`) AS day,
            ROUND(SUM(gp.ActualGenerationOutput * 0.25 * ep.`Price[Currency/MWh]`) /
                  NULLIF(SUM(gp.ActualGenerationOutput * 0.25), 0), 2) AS capture_price,
            ROUND(SUM(gp.ActualGenerationOutput * 0.25 * 
                  CASE WHEN ep.`Price[Currency/MWh]` < 0 THEN 0 ELSE ep.`Price[Currency/MWh]` END) /
                  NULLIF(SUM(gp.ActualGenerationOutput * 0.25), 0), 2) AS capture_price_floor0,
            ROUND(100.0 * SUM(CASE WHEN ep.`Price[Currency/MWh]` < 0 THEN gp.ActualGenerationOutput * 0.25 ELSE 0 END) /
                  NULLIF(SUM(gp.ActualGenerationOutput * 0.25), 0), 2) AS solar_at_neg_price_pct
        FROM temp_prices_current_month ep
        JOIN generation_per_type gp
            ON ep.AreaCode = gp.AreaCode
            AND ep.`DateTime(UTC)` = gp.`DateTime(UTC)`
        WHERE gp.ProductionType = 'Solar'
          AND gp.ActualGenerationOutput > 0
        GROUP BY ep.AreaDisplayName, DAY(ep.`DateTime(UTC)`)
    """)
    conn.commit()
    
    cursor.execute("""
        UPDATE summary_daily sd
        JOIN temp_daily_capture tc ON sd.country = tc.country AND sd.day = tc.day
        SET sd.capture_price = COALESCE(tc.capture_price, 0),
            sd.capture_price_floor0 = COALESCE(tc.capture_price_floor0, 0),
            sd.solar_at_neg_price_pct = COALESCE(tc.solar_at_neg_price_pct, 0)
        WHERE sd.year = %s AND sd.month = %s
    """, (current_year, current_month))
    conn.commit()
    
    cursor.execute("""
        UPDATE summary_daily
        SET capture_rate = ROUND(100.0 * capture_price / NULLIF(avg_market_price, 0), 2)
        WHERE year = %s AND month = %s AND avg_market_price > 0
    """, (current_year, current_month))
    conn.commit()
    print("   ✅ Daily metrics complete", flush=True)
    
    # =========================================================
    # STEP 2: Update summary_monthly for current month only
    # =========================================================
    print(f"\n2. Updating summary_monthly for {current_year}-{current_month:02d}...", flush=True)
    
    # Delete and recalculate current month's monthly summary
    cursor.execute("""
        DELETE FROM summary_monthly 
        WHERE year = %s AND month = %s
    """, (current_year, current_month))
    conn.commit()
    
    # Aggregate from daily data
    cursor.execute("""
        INSERT INTO summary_monthly (year, country, month, neg_hours, avg_market_price, 
                                     capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct)
        SELECT 
            year,
            country,
            month,
            SUM(neg_hours),
            ROUND(AVG(avg_market_price), 2),
            ROUND(AVG(capture_price), 2),
            ROUND(AVG(capture_price_floor0), 2),
            ROUND(AVG(capture_rate), 2),
            ROUND(AVG(solar_at_neg_price_pct), 2)
        FROM summary_daily
        WHERE year = %s AND month = %s
        GROUP BY year, country, month
    """, (current_year, current_month))
    inserted_monthly = cursor.rowcount
    conn.commit()
    print(f"   Inserted/updated {inserted_monthly} monthly rows", flush=True)
    
    # =========================================================
    # STEP 3: Update summary_yearly for current year only
    # =========================================================
    print(f"\n3. Updating summary_yearly for {current_year}...", flush=True)
    
    # Delete current year's yearly summary
    cursor.execute("""
        DELETE FROM summary_yearly WHERE year = %s
    """, (current_year,))
    conn.commit()
    
    # Aggregate from all monthly data for current year
    cursor.execute("""
        INSERT INTO summary_yearly (year, country, total_neg_hours, avg_market_price,
                                    capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct)
        SELECT 
            year,
            country,
            SUM(neg_hours),
            ROUND(AVG(avg_market_price), 2),
            ROUND(AVG(capture_price), 2),
            ROUND(AVG(capture_price_floor0), 2),
            ROUND(AVG(capture_rate), 2),
            ROUND(AVG(solar_at_neg_price_pct), 2)
        FROM summary_monthly
        WHERE year = %s
        GROUP BY year, country
    """, (current_year,))
    inserted_yearly = cursor.rowcount
    conn.commit()
    print(f"   Inserted {inserted_yearly} yearly rows", flush=True)
    
    # =========================================================
    # STEP 4: Update summary_total (all years combined)
    # =========================================================
    print("\n4. Updating summary_total...", flush=True)
    cursor.execute("TRUNCATE TABLE summary_total")
    cursor.execute("""
        INSERT INTO summary_total (id, total_neg_hours, avg_market_price,
                                   capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct)
        SELECT 1, SUM(total_neg_hours), ROUND(AVG(avg_market_price), 2),
               ROUND(AVG(capture_price), 2), ROUND(AVG(capture_price_floor0), 2),
               ROUND(AVG(capture_rate), 2), ROUND(AVG(solar_at_neg_price_pct), 2)
        FROM summary_yearly
    """)
    conn.commit()
    print("   ✅ Total summary updated", flush=True)
    
    # Cleanup
    cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_prices_current_month")
    cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_daily_capture")
    conn.commit()
    
    # =========================================================
    # RESULTS
    # =========================================================
    print("\n" + "=" * 60, flush=True)
    print("Summary Update Complete!", flush=True)
    print("=" * 60, flush=True)
    
    cursor.execute("""
        SELECT country, neg_hours, avg_market_price, capture_price 
        FROM summary_monthly 
        WHERE year = %s AND month = %s AND country = 'DE-LU'
    """, (current_year, current_month))
    r = cursor.fetchone()
    if r:
        print(f"\nDE-LU {current_year}-{current_month:02d}: neg={r[1]}h, avg=€{r[2]}, cap=€{r[3]}", flush=True)
    
    cursor.execute("""
        SELECT COUNT(*) FROM summary_daily WHERE year = %s AND month = %s
    """, (current_year, current_month))
    print(f"Total daily records for {current_year}-{current_month:02d}: {cursor.fetchone()[0]}", flush=True)
    
    cursor.close()
    conn.close()
    print("\n✅ Done!", flush=True)

if __name__ == "__main__":
    main()
