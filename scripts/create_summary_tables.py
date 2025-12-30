"""
Create summary tables for instant dashboard loading
Includes: neg_hours, avg_market_price, capture_price, capture_price_floor0, 
          capture_rate, solar_at_neg_price_pct
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
          print("=" * 60, flush=True)
          print("Creating Summary Tables with All Metrics", flush=True)
          print("=" * 60, flush=True)

          conn = mysql.connector.connect(**DB_CONFIG)
          cursor = conn.cursor()

          print("✅ Connected", flush=True)
          print("DB_HOST:", DB_CONFIG["host"], flush=True)
          print("DB_NAME:", DB_CONFIG["database"], flush=True)

          cursor.execute("SELECT CURRENT_USER(), DATABASE(), @@hostname, @@port, NOW()")
          print("Server:", cursor.fetchone(), flush=True)

          cursor.execute("SELECT COUNT(*) FROM energy_prices")
          print("energy_prices rows:", cursor.fetchone()[0], flush=True)

          cursor.execute(
                    "SELECT COUNT(*) FROM generation_per_type WHERE ProductionType='Solar'"
    )
          print("generation_per_type Solar rows:", cursor.fetchone()[0], flush=True)

          print(
                  "Build started at UTC:",
                  datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                  flush=True,
    )

    # =========================================================
    # STEP 1: Create summary_monthly table
    # =========================================================
          print("\n1. Creating summary_monthly table structure...", flush=True)

          cursor.execute("DROP TABLE IF EXISTS summary_monthly")

          create_sql = """
          CREATE TABLE summary_monthly (
          id INT AUTO_INCREMENT PRIMARY KEY,
          country VARCHAR(100) NOT NULL,
          month TINYINT NOT NULL,
          neg_hours DECIMAL(10,2) DEFAULT 0,
          avg_market_price DECIMAL(10,2) DEFAULT 0,
          capture_price DECIMAL(10,2) DEFAULT 0,
          capture_price_floor0 DECIMAL(10,2) DEFAULT 0,
          capture_rate DECIMAL(10,2) DEFAULT 0,
          solar_at_neg_price_pct DECIMAL(10,2) DEFAULT 0,
          UNIQUE KEY unique_country_month (country, month)
    )
    """

    cursor.execute(create_sql)
    conn.commit()

    
    # =========================================================
    # STEP 2: Calculate neg_hours and avg_market_price (from energy_prices only)
    # =========================================================
    print("\n2. Calculating neg_hours and avg_market_price...", flush=True)
    cursor.execute("""
        INSERT INTO summary_monthly (country, month, neg_hours, avg_market_price)
        WITH prices_raw AS (
            SELECT
                AreaCode,
                AreaDisplayName,
                `DateTime(UTC)`,
                ResolutionCode,
                `Price[Currency/MWh]`,
                source_month,
                SUM(CASE WHEN ResolutionCode = 'PT60M' THEN 1 ELSE 0 END) 
                    OVER (PARTITION BY AreaCode, `DateTime(UTC)`) AS cnt_60m_same_ts
            FROM energy_prices
            WHERE
                ContractType = 'Day-ahead'
                AND (`Sequence` IS NULL OR `Sequence` NOT IN ('2', '3'))
        ),
        prices_dedup AS (
            SELECT *
            FROM prices_raw
            WHERE
                ResolutionCode = 'PT60M'
                OR (ResolutionCode = 'PT15M' AND cnt_60m_same_ts = 0)
        )
        SELECT
            AreaDisplayName AS country,
            source_month AS month,
            SUM(
                CASE
                    WHEN ResolutionCode = 'PT60M' AND `Price[Currency/MWh]` < 0 THEN 1.0
                    WHEN ResolutionCode = 'PT15M' AND `Price[Currency/MWh]` < 0 THEN 0.25
                    ELSE 0.0
                END
            ) AS neg_hours,
            ROUND(AVG(`Price[Currency/MWh]`), 2) AS avg_market_price
        FROM prices_dedup
        GROUP BY AreaDisplayName, source_month
    """)
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM summary_monthly")
    print(f"   Inserted {cursor.fetchone()[0]} rows.", flush=True)
    
    # =========================================================
    # STEP 3: Calculate capture metrics (requires JOIN with generation_per_type)
    # =========================================================
    print("\n3. Calculating capture price metrics (this may take a few minutes)...", flush=True)
    
    # Create temporary table with joined data
    print("   3a. Creating joined price+generation data...", flush=True)
    cursor.execute("DROP TABLE IF EXISTS temp_joined")
    cursor.execute("""
        CREATE TABLE temp_joined AS
        WITH prices_raw AS (
            SELECT
                AreaCode,
                AreaDisplayName,
                `DateTime(UTC)`,
                ResolutionCode,
                `Price[Currency/MWh]`,
                source_month,
                SUM(CASE WHEN ResolutionCode = 'PT60M' THEN 1 ELSE 0 END) 
                    OVER (PARTITION BY AreaCode, `DateTime(UTC)`) AS cnt_60m_same_ts
            FROM energy_prices
            WHERE
                ContractType = 'Day-ahead'
                AND (`Sequence` IS NULL OR `Sequence` NOT IN ('2', '3'))
        ),
        prices_dedup AS (
            SELECT *
            FROM prices_raw
            WHERE
                ResolutionCode = 'PT60M'
                OR (ResolutionCode = 'PT15M' AND cnt_60m_same_ts = 0)
        )
        SELECT
            ep.AreaDisplayName AS country,
            ep.source_month AS month,
            CASE
                WHEN ep.ResolutionCode = 'PT15M' THEN 0.25
                WHEN ep.ResolutionCode = 'PT60M' THEN 1.0
                ELSE 1.0
            END AS interval_hours,
            ep.`Price[Currency/MWh]` AS price_raw,
            gp.ActualGenerationOutput AS gen_mw
        FROM prices_dedup ep
        JOIN generation_per_type gp
            ON ep.AreaCode = gp.AreaCode
            AND ep.`DateTime(UTC)` = gp.`DateTime(UTC)`
            AND ep.source_month = gp.source_month
        WHERE
            gp.ProductionType = 'Solar'
            AND gp.ActualGenerationOutput > 0
    """)
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM temp_joined")
    joined_count = cursor.fetchone()[0]
    print(f"   Joined rows: {joined_count}", flush=True)
    
    if joined_count > 0:
        # Calculate capture metrics
        print("   3b. Calculating capture metrics per country/month...", flush=True)
        cursor.execute("""
            CREATE TABLE temp_capture AS
            SELECT
                country,
                month,
                ROUND(
                    SUM(gen_mw * interval_hours * price_raw) / NULLIF(SUM(gen_mw * interval_hours), 0),
                    2
                ) AS capture_price,
                ROUND(
                    SUM(gen_mw * interval_hours * CASE WHEN price_raw < 0 THEN 0 ELSE price_raw END) 
                    / NULLIF(SUM(gen_mw * interval_hours), 0),
                    2
                ) AS capture_price_floor0,
                ROUND(
                    100.0 * SUM(CASE WHEN price_raw < 0 THEN gen_mw * interval_hours ELSE 0 END)
                    / NULLIF(SUM(gen_mw * interval_hours), 0),
                    2
                ) AS solar_at_neg_price_pct
            FROM temp_joined
            GROUP BY country, month
        """)
        conn.commit()
        
        # Update summary_monthly with capture metrics
        print("   3c. Updating summary_monthly with capture metrics...", flush=True)
        cursor.execute("""
            UPDATE summary_monthly sm
            JOIN temp_capture tc ON sm.country = tc.country AND sm.month = tc.month
            SET 
                sm.capture_price = COALESCE(tc.capture_price, 0),
                sm.capture_price_floor0 = COALESCE(tc.capture_price_floor0, 0),
                sm.solar_at_neg_price_pct = COALESCE(tc.solar_at_neg_price_pct, 0)
        """)
        conn.commit()
        
        # Calculate capture rate
        print("   3d. Calculating capture rate...", flush=True)
        cursor.execute("""
            UPDATE summary_monthly
            SET capture_rate = ROUND(
                100.0 * capture_price / NULLIF(avg_market_price, 0),
                2
            )
            WHERE avg_market_price > 0
        """)
        conn.commit()
        
        # Cleanup temp tables
        cursor.execute("DROP TABLE IF EXISTS temp_joined")
        cursor.execute("DROP TABLE IF EXISTS temp_capture")
        conn.commit()
    else:
        print("   WARNING: No joined data found. Check if generation_per_type has matching records.", flush=True)
    
    print("   Done.", flush=True)
    
    # =========================================================
    # STEP 4: Create summary_yearly table
    # =========================================================
    print("\n4. Creating summary_yearly table...", flush=True)
    cursor.execute("DROP TABLE IF EXISTS summary_yearly")
    cursor.execute("""
        CREATE TABLE summary_yearly (
            id INT AUTO_INCREMENT PRIMARY KEY,
            country VARCHAR(100) NOT NULL UNIQUE,
            total_neg_hours DECIMAL(10,2) DEFAULT 0,
            avg_market_price DECIMAL(10,2) DEFAULT 0,
            capture_price DECIMAL(10,2) DEFAULT 0,
            capture_price_floor0 DECIMAL(10,2) DEFAULT 0,
            capture_rate DECIMAL(10,2) DEFAULT 0,
            solar_at_neg_price_pct DECIMAL(10,2) DEFAULT 0,
            INDEX idx_country (country)
        )
    """)
    conn.commit()
    
    cursor.execute("""
        INSERT INTO summary_yearly (country, total_neg_hours, avg_market_price, 
                                    capture_price, capture_price_floor0, capture_rate, solar_at_neg_price_pct)
        SELECT 
            country,
            SUM(neg_hours) AS total_neg_hours,
            ROUND(AVG(avg_market_price), 2) AS avg_market_price,
            ROUND(AVG(capture_price), 2) AS capture_price,
            ROUND(AVG(capture_price_floor0), 2) AS capture_price_floor0,
            ROUND(AVG(capture_rate), 2) AS capture_rate,
            ROUND(AVG(solar_at_neg_price_pct), 2) AS solar_at_neg_price_pct
        FROM summary_monthly
        GROUP BY country
    """)
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM summary_yearly")
    print(f"   Inserted {cursor.fetchone()[0]} rows.", flush=True)
    
    # =========================================================
    # STEP 5: Create summary_total table
    # =========================================================
    print("\n5. Creating summary_total table...", flush=True)
    cursor.execute("DROP TABLE IF EXISTS summary_total")
    cursor.execute("""
        CREATE TABLE summary_total (
            id INT PRIMARY KEY DEFAULT 1,
            total_neg_hours DECIMAL(12,2) DEFAULT 0,
            avg_market_price DECIMAL(10,2) DEFAULT 0,
            capture_price DECIMAL(10,2) DEFAULT 0,
            capture_price_floor0 DECIMAL(10,2) DEFAULT 0,
            capture_rate DECIMAL(10,2) DEFAULT 0,
            solar_at_neg_price_pct DECIMAL(10,2) DEFAULT 0
        )
    """)
    conn.commit()
    
    cursor.execute("""
        INSERT INTO summary_total (id, total_neg_hours, avg_market_price, 
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
    print("   Done.", flush=True)
    
    # =========================================================
    # STEP 6: Create summary_daily table
    # =========================================================
    print("\n6. Creating summary_daily table (for single zone + single month view)...", flush=True)
    cursor.execute("DROP TABLE IF EXISTS summary_daily")
    cursor.execute("""
        CREATE TABLE summary_daily (
            id INT AUTO_INCREMENT PRIMARY KEY,
            country VARCHAR(100) NOT NULL,
            month TINYINT NOT NULL,
            day TINYINT NOT NULL,
            neg_hours DECIMAL(10,2) DEFAULT 0,
            avg_market_price DECIMAL(10,2) DEFAULT 0,
            capture_price DECIMAL(10,2) DEFAULT 0,
            capture_price_floor0 DECIMAL(10,2) DEFAULT 0,
            capture_rate DECIMAL(10,2) DEFAULT 0,
            solar_at_neg_price_pct DECIMAL(10,2) DEFAULT 0,
            UNIQUE KEY unique_country_month_day (country, month, day),
            INDEX idx_country_month (country, month)
        )
    """)
    conn.commit()
    print("   Table created.", flush=True)
    
    # Insert daily neg_hours and avg_market_price
    print("   6a. Calculating daily neg_hours and avg_market_price...", flush=True)
    cursor.execute("""
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
            WHERE
                ContractType = 'Day-ahead'
                AND (`Sequence` IS NULL OR `Sequence` NOT IN ('2', '3'))
        ),
        prices_dedup AS (
            SELECT *
            FROM prices_raw
            WHERE
                ResolutionCode = 'PT60M'
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
    """)
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM summary_daily")
    print(f"   Inserted {cursor.fetchone()[0]} rows.", flush=True)
    
    # Calculate daily capture metrics
    if joined_count > 0:
        print("   6b. Calculating daily capture metrics...", flush=True)
        cursor.execute("DROP TABLE IF EXISTS temp_daily_capture")
        cursor.execute("""
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
                WHERE
                    ContractType = 'Day-ahead'
                    AND (`Sequence` IS NULL OR `Sequence` NOT IN ('2', '3'))
            ),
            prices_dedup AS (
                SELECT *
                FROM prices_raw
                WHERE
                    ResolutionCode = 'PT60M'
                    OR (ResolutionCode = 'PT15M' AND cnt_60m_same_ts = 0)
            ),
            joined AS (
                SELECT
                    ep.AreaDisplayName AS country,
                    ep.source_month AS month,
                    DAY(ep.`DateTime(UTC)`) AS day,
                    CASE
                        WHEN ep.ResolutionCode = 'PT15M' THEN 0.25
                        ELSE 1.0
                    END AS interval_hours,
                    ep.`Price[Currency/MWh]` AS price_raw,
                    gp.ActualGenerationOutput AS gen_mw
                FROM prices_dedup ep
                JOIN generation_per_type gp
                    ON ep.AreaCode = gp.AreaCode
                    AND ep.`DateTime(UTC)` = gp.`DateTime(UTC)`
                    AND ep.source_month = gp.source_month
                WHERE gp.ProductionType = 'Solar' AND gp.ActualGenerationOutput > 0
            )
            SELECT
                country, month, day,
                ROUND(SUM(gen_mw * interval_hours * price_raw) / NULLIF(SUM(gen_mw * interval_hours), 0), 2) AS capture_price,
                ROUND(SUM(gen_mw * interval_hours * CASE WHEN price_raw < 0 THEN 0 ELSE price_raw END) / NULLIF(SUM(gen_mw * interval_hours), 0), 2) AS capture_price_floor0,
                ROUND(100.0 * SUM(CASE WHEN price_raw < 0 THEN gen_mw * interval_hours ELSE 0 END) / NULLIF(SUM(gen_mw * interval_hours), 0), 2) AS solar_at_neg_price_pct
            FROM joined
            GROUP BY country, month, day
        """)
        conn.commit()
        
        print("   6c. Updating summary_daily with capture metrics...", flush=True)
        cursor.execute("""
            UPDATE summary_daily sd
            JOIN temp_daily_capture tc ON sd.country = tc.country AND sd.month = tc.month AND sd.day = tc.day
            SET 
                sd.capture_price = COALESCE(tc.capture_price, 0),
                sd.capture_price_floor0 = COALESCE(tc.capture_price_floor0, 0),
                sd.solar_at_neg_price_pct = COALESCE(tc.solar_at_neg_price_pct, 0)
        """)
        conn.commit()
        
        cursor.execute("""
            UPDATE summary_daily
            SET capture_rate = ROUND(100.0 * capture_price / NULLIF(avg_market_price, 0), 2)
            WHERE avg_market_price > 0
        """)
        conn.commit()
        
        cursor.execute("DROP TABLE IF EXISTS temp_daily_capture")
        conn.commit()
    
    print("   Done.", flush=True)
    
    # =========================================================
    # RESULTS
    # =========================================================
    print("\n" + "=" * 60, flush=True)
    print("Summary Tables Created Successfully!", flush=True)
    print("=" * 60, flush=True)
    
    cursor.execute("SELECT * FROM summary_total")
    row = cursor.fetchone()
    print(f"\nOverall Totals (All Zones, Full Year):")
    print(f"  Negative Hours:        {row[1]} hrs")
    print(f"  Avg Market Price:      €{row[2]}/MWh")
    print(f"  Capture Price:         €{row[3]}/MWh")
    print(f"  Capture Price (Floor): €{row[4]}/MWh")
    print(f"  Capture Rate:          {row[5]}%")
    print(f"  Solar @ Neg Price:     {row[6]}%")
    
    print(f"\nTop 5 zones by negative hours:")
    cursor.execute("""
        SELECT country, total_neg_hours, avg_market_price, capture_price, capture_rate 
        FROM summary_yearly 
        ORDER BY total_neg_hours DESC 
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} hrs, €{row[2]} avg, €{row[3]} capture, {row[4]}% rate")
    
    cursor.close()
    conn.close()
    print("\n✅ Done!", flush=True)

if __name__ == "__main__":
    main()
