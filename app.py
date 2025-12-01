"""
European Energy Market Dashboard
Visualizes negative price hours, capture prices, avg market prices, 
and capture prices with floor at 0 for each bidding zone and month.
"""

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import duckdb
import json
from pathlib import Path

app = FastAPI(title="European Energy Market Dashboard")

# Get the directory where the CSV files are located
DATA_DIR = Path(__file__).parent


def format_country_name(name: str) -> str:
    """
    Transform area display names to more readable country/zone names.
    """
    # Direct replacements
    name_map = {
        "DE-LU": "Germany-Luxembourg",
        "DK1": "Denmark West (DK1)",
        "DK2": "Denmark East (DK2)",
        "NO1": "Norway South-East (NO1)",
        "NO2": "Norway South-West (NO2)",
        "NO2NSL": "Norway NSL (NO2NSL)",
        "NO3": "Norway Central (NO3)",
        "NO4": "Norway North (NO4)",
        "NO5": "Norway West (NO5)",
        "SE1": "Sweden North (SE1)",
        "SE2": "Sweden Central-North (SE2)",
        "SE3": "Sweden Central-South (SE3)",
        "SE4": "Sweden South (SE4)",
        "IE(SEM)": "Ireland (SEM)",
        "UA-IPS": "Ukraine (IPS)",
    }
    
    # Check for direct mapping
    if name in name_map:
        return name_map[name]
    
    # Replace IT- prefix with Italy-
    if name.startswith("IT-"):
        return "Italy-" + name[3:]
    
    return name


def get_market_data():
    """
    Run the comprehensive SQL query to calculate all energy market metrics
    per bidding zone and month.
    """
    conn = duckdb.connect(":memory:")
    
    query = f"""
    WITH raw AS (
        SELECT
            *,
            CAST(SUBSTR(filename, LENGTH('{DATA_DIR}/') + 6, 2) AS INTEGER) AS month,
            COUNT(*) FILTER (
                WHERE ResolutionCode = 'PT15M'
            ) OVER (
                PARTITION BY AreaDisplayName, "DateTime(UTC)"
            ) AS cnt_15m_same_ts
        FROM read_csv_auto(
            '{DATA_DIR}/2025_??_EnergyPrices_12.1.D_r3.csv', 
            filename=true
        )
        WHERE 
            ContractType = 'Day-ahead'
            AND Sequence NOT IN ('2', '3')
    ),
    
    -- Negative hours aggregation
    neg_hours_agg AS (
        SELECT
            AreaDisplayName AS country,
            month,
            SUM(
                CASE
                    WHEN ResolutionCode = 'PT15M' AND "Price[Currency/MWh]" < 0 THEN 0.25
                    WHEN ResolutionCode = 'PT60M' AND cnt_15m_same_ts = 0 AND "Price[Currency/MWh]" < 0 THEN 1
                    ELSE 0
                END
            ) AS neg_hours
        FROM raw
        GROUP BY AreaDisplayName, month
    ),
    
    -- Average market price per country/month
    avg_price_agg AS (
        SELECT
            AreaDisplayName AS country,
            month,
            AVG("Price[Currency/MWh]") AS avg_price
        FROM raw
        WHERE 
            ResolutionCode = 'PT60M' OR cnt_15m_same_ts = 0
        GROUP BY AreaDisplayName, month
    ),
    
    prices_raw AS (
        SELECT
            ep.*,
            CAST(SUBSTR(ep.filename, LENGTH('{DATA_DIR}/') + 6, 2) AS INTEGER) AS month,
            COUNT(*) FILTER (
                WHERE ResolutionCode = 'PT60M'
            ) OVER (
                PARTITION BY AreaDisplayName, "DateTime(UTC)"
            ) AS cnt_60m_same_ts
        FROM read_csv_auto('{DATA_DIR}/2025_??_EnergyPrices_12.1.D_r3.csv', filename=true) AS ep
        WHERE
            ep.ContractType = 'Day-ahead'
            AND ep.Sequence NOT IN ('2', '3')
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
            ep."DateTime(UTC)",
            ep.ResolutionCode,
            ep.month,
            ep."Price[Currency/MWh]" AS price_raw,
            ag."ActualGenerationOutput" AS gen_mw,
            CASE
                WHEN ep.ResolutionCode = 'PT15M' THEN 0.25
                WHEN ep.ResolutionCode = 'PT60M' THEN 1.0
                ELSE 1.0
            END AS interval_hours
        FROM prices_dedup AS ep
        JOIN read_csv_auto(
                '{DATA_DIR}/2025_??_AggregatedGenerationPerType_16.1.B_C.csv',
                strict_mode=false,
                ignore_errors=true,
                filename=true
            ) AS ag
          ON ep.AreaCode = ag.AreaCode
         AND ep."DateTime(UTC)" = ag."DateTime"
         AND ep.month = CAST(SUBSTR(ag.filename, LENGTH('{DATA_DIR}/') + 6, 2) AS INTEGER)
        WHERE
            ag.ProductionType = 'Solar'
            AND ag."ActualGenerationOutput" > 0
    ),
    
    -- Capture price calculation per month
    capture_price AS (
        SELECT
            country,
            month,
            ROUND(
                SUM(gen_mw * interval_hours * price_raw)
                / NULLIF(SUM(gen_mw * interval_hours), 0),
                2
            ) AS capture_price
        FROM joined
        GROUP BY country, month
    ),
    
    -- Floor-priced capture price per month
    capture_floor0 AS (
        SELECT
            country,
            month,
            ROUND(
                SUM(gen_mw * interval_hours * CASE WHEN price_raw < 0 THEN 0 ELSE price_raw END)
                / NULLIF(SUM(gen_mw * interval_hours), 0),
                2
            ) AS capture_price_floor0
        FROM joined
        GROUP BY country, month
    ),
    
    -- Solar volume share at negative prices (%)
    solar_neg_share AS (
        SELECT
            country,
            month,
            ROUND(
                100.0 * SUM(CASE WHEN price_raw < 0 THEN gen_mw * interval_hours ELSE 0 END)
                / NULLIF(SUM(gen_mw * interval_hours), 0),
                2
            ) AS solar_at_neg_price_pct
        FROM joined
        GROUP BY country, month
    )
    
    SELECT
        COALESCE(nh.country, ap.country, cp.country, cf.country, sn.country) AS country,
        COALESCE(nh.month, ap.month, cp.month, cf.month, sn.month) AS month,
        COALESCE(nh.neg_hours, 0) AS neg_hours,
        ROUND(COALESCE(ap.avg_price, 0), 2) AS avg_market_price,
        COALESCE(cp.capture_price, 0) AS capture_price,
        COALESCE(cf.capture_price_floor0, 0) AS capture_price_floor0,
        COALESCE(sn.solar_at_neg_price_pct, 0) AS solar_at_neg_price_pct
    FROM neg_hours_agg nh
    FULL OUTER JOIN avg_price_agg ap ON nh.country = ap.country AND nh.month = ap.month
    FULL OUTER JOIN capture_price cp ON COALESCE(nh.country, ap.country) = cp.country 
                                     AND COALESCE(nh.month, ap.month) = cp.month
    FULL OUTER JOIN capture_floor0 cf ON COALESCE(nh.country, ap.country, cp.country) = cf.country 
                                      AND COALESCE(nh.month, ap.month, cp.month) = cf.month
    FULL OUTER JOIN solar_neg_share sn ON COALESCE(nh.country, ap.country, cp.country, cf.country) = sn.country 
                                       AND COALESCE(nh.month, ap.month, cp.month, cf.month) = sn.month
    WHERE COALESCE(nh.country, ap.country, cp.country, cf.country, sn.country) IS NOT NULL
    ORDER BY country, month;
    """
    
    result = conn.execute(query).fetchall()
    columns = ['country', 'month', 'neg_hours', 'avg_market_price', 'capture_price', 'capture_price_floor0', 'solar_at_neg_price_pct']
    
    data = []
    for row in result:
        record = dict(zip(columns, row))
        record['country'] = format_country_name(record['country'])
        data.append(record)
    
    conn.close()
    
    return data


def get_countries_list():
    """Get list of unique countries/bidding zones."""
    conn = duckdb.connect(":memory:")
    query = f"""
    SELECT DISTINCT AreaDisplayName 
    FROM read_csv_auto('{DATA_DIR}/2025_01_EnergyPrices_12.1.D_r3.csv')
    WHERE ContractType = 'Day-ahead'
    ORDER BY AreaDisplayName
    """
    result = conn.execute(query).fetchall()
    conn.close()
    # Apply name formatting and sort alphabetically
    formatted_names = sorted([format_country_name(row[0]) for row in result])
    return formatted_names


def get_daily_data():
    """
    Run the comprehensive SQL query to calculate all energy market metrics
    per bidding zone and day.
    """
    conn = duckdb.connect(":memory:")
    
    query = f"""
    WITH raw AS (
        SELECT
            *,
            CAST(SUBSTR(filename, LENGTH('{DATA_DIR}/') + 6, 2) AS INTEGER) AS month,
            CAST("DateTime(UTC)" AS DATE) AS day_date,
            COUNT(*) FILTER (
                WHERE ResolutionCode = 'PT15M'
            ) OVER (
                PARTITION BY AreaDisplayName, "DateTime(UTC)"
            ) AS cnt_15m_same_ts
        FROM read_csv_auto(
            '{DATA_DIR}/2025_??_EnergyPrices_12.1.D_r3.csv', 
            filename=true
        )
        WHERE 
            ContractType = 'Day-ahead'
            AND Sequence NOT IN ('2', '3')
    ),
    
    -- Negative hours aggregation per day
    neg_hours_agg AS (
        SELECT
            AreaDisplayName AS country,
            month,
            day_date,
            SUM(
                CASE
                    WHEN ResolutionCode = 'PT15M' AND "Price[Currency/MWh]" < 0 THEN 0.25
                    WHEN ResolutionCode = 'PT60M' AND cnt_15m_same_ts = 0 AND "Price[Currency/MWh]" < 0 THEN 1
                    ELSE 0
                END
            ) AS neg_hours
        FROM raw
        GROUP BY AreaDisplayName, month, day_date
    ),
    
    -- Average market price per country/day
    avg_price_agg AS (
        SELECT
            AreaDisplayName AS country,
            month,
            day_date,
            AVG("Price[Currency/MWh]") AS avg_price
        FROM raw
        WHERE 
            ResolutionCode = 'PT60M' OR cnt_15m_same_ts = 0
        GROUP BY AreaDisplayName, month, day_date
    ),
    
    prices_raw AS (
        SELECT
            ep.*,
            CAST(SUBSTR(ep.filename, LENGTH('{DATA_DIR}/') + 6, 2) AS INTEGER) AS month,
            CAST(ep."DateTime(UTC)" AS DATE) AS day_date,
            COUNT(*) FILTER (
                WHERE ResolutionCode = 'PT60M'
            ) OVER (
                PARTITION BY AreaDisplayName, "DateTime(UTC)"
            ) AS cnt_60m_same_ts
        FROM read_csv_auto('{DATA_DIR}/2025_??_EnergyPrices_12.1.D_r3.csv', filename=true) AS ep
        WHERE
            ep.ContractType = 'Day-ahead'
            AND ep.Sequence NOT IN ('2', '3')
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
            ep."DateTime(UTC)",
            ep.ResolutionCode,
            ep.month,
            ep.day_date,
            ep."Price[Currency/MWh]" AS price_raw,
            ag."ActualGenerationOutput" AS gen_mw,
            CASE
                WHEN ep.ResolutionCode = 'PT15M' THEN 0.25
                WHEN ep.ResolutionCode = 'PT60M' THEN 1.0
                ELSE 1.0
            END AS interval_hours
        FROM prices_dedup AS ep
        JOIN read_csv_auto(
                '{DATA_DIR}/2025_??_AggregatedGenerationPerType_16.1.B_C.csv',
                strict_mode=false,
                ignore_errors=true,
                filename=true
            ) AS ag
          ON ep.AreaCode = ag.AreaCode
         AND ep."DateTime(UTC)" = ag."DateTime"
         AND ep.month = CAST(SUBSTR(ag.filename, LENGTH('{DATA_DIR}/') + 6, 2) AS INTEGER)
        WHERE
            ag.ProductionType = 'Solar'
            AND ag."ActualGenerationOutput" > 0
    ),
    
    -- Capture price calculation per day
    capture_price AS (
        SELECT
            country,
            month,
            day_date,
            ROUND(
                SUM(gen_mw * interval_hours * price_raw)
                / NULLIF(SUM(gen_mw * interval_hours), 0),
                2
            ) AS capture_price
        FROM joined
        GROUP BY country, month, day_date
    ),
    
    -- Floor-priced capture price per day
    capture_floor0 AS (
        SELECT
            country,
            month,
            day_date,
            ROUND(
                SUM(gen_mw * interval_hours * CASE WHEN price_raw < 0 THEN 0 ELSE price_raw END)
                / NULLIF(SUM(gen_mw * interval_hours), 0),
                2
            ) AS capture_price_floor0
        FROM joined
        GROUP BY country, month, day_date
    ),
    
    -- Solar volume share at negative prices per day
    solar_neg_share AS (
        SELECT
            country,
            month,
            day_date,
            ROUND(
                100.0 * SUM(CASE WHEN price_raw < 0 THEN gen_mw * interval_hours ELSE 0 END)
                / NULLIF(SUM(gen_mw * interval_hours), 0),
                2
            ) AS solar_at_neg_price_pct
        FROM joined
        GROUP BY country, month, day_date
    )
    
    SELECT
        COALESCE(nh.country, ap.country, cp.country, cf.country, sn.country) AS country,
        COALESCE(nh.month, ap.month, cp.month, cf.month, sn.month) AS month,
        COALESCE(nh.day_date, ap.day_date, cp.day_date, cf.day_date, sn.day_date) AS day_date,
        EXTRACT(DAY FROM COALESCE(nh.day_date, ap.day_date, cp.day_date, cf.day_date, sn.day_date)) AS day,
        COALESCE(nh.neg_hours, 0) AS neg_hours,
        ROUND(COALESCE(ap.avg_price, 0), 2) AS avg_market_price,
        COALESCE(cp.capture_price, 0) AS capture_price,
        COALESCE(cf.capture_price_floor0, 0) AS capture_price_floor0,
        COALESCE(sn.solar_at_neg_price_pct, 0) AS solar_at_neg_price_pct
    FROM neg_hours_agg nh
    FULL OUTER JOIN avg_price_agg ap ON nh.country = ap.country AND nh.day_date = ap.day_date
    FULL OUTER JOIN capture_price cp ON COALESCE(nh.country, ap.country) = cp.country 
                                     AND COALESCE(nh.day_date, ap.day_date) = cp.day_date
    FULL OUTER JOIN capture_floor0 cf ON COALESCE(nh.country, ap.country, cp.country) = cf.country 
                                      AND COALESCE(nh.day_date, ap.day_date, cp.day_date) = cf.day_date
    FULL OUTER JOIN solar_neg_share sn ON COALESCE(nh.country, ap.country, cp.country, cf.country) = sn.country 
                                       AND COALESCE(nh.day_date, ap.day_date, cp.day_date, cf.day_date) = sn.day_date
    WHERE COALESCE(nh.country, ap.country, cp.country, cf.country, sn.country) IS NOT NULL
    ORDER BY country, day_date;
    """
    
    result = conn.execute(query).fetchall()
    columns = ['country', 'month', 'day_date', 'day', 'neg_hours', 'avg_market_price', 'capture_price', 'capture_price_floor0', 'solar_at_neg_price_pct']
    
    data = []
    for row in result:
        record = dict(zip(columns, row))
        record['country'] = format_country_name(record['country'])
        # Convert date to string for JSON serialization
        if record['day_date']:
            record['day_date'] = str(record['day_date'])
        data.append(record)
    
    conn.close()
    
    return data


@app.get("/api/data")
def api_data():
    """API endpoint returning all market data as JSON."""
    data = get_market_data()
    return {"data": data}


@app.get("/api/daily-data")
def api_daily_data():
    """API endpoint returning daily market data as JSON."""
    data = get_daily_data()
    return {"data": data}


@app.get("/api/countries")
def api_countries():
    """API endpoint returning list of countries."""
    countries = get_countries_list()
    return {"countries": countries}


@app.get("/", response_class=HTMLResponse)
def index():
    """Serve the main dashboard HTML page."""
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>European Energy Market Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Sora:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #0a0e14;
            --bg-secondary: #111820;
            --bg-tertiary: #1a2332;
            --accent-cyan: #00f5d4;
            --accent-magenta: #f72585;
            --accent-yellow: #fee440;
            --accent-orange: #ff6b35;
            --accent-purple: #9d4edd;
            --text-primary: #e8eaed;
            --text-secondary: #9aa5b1;
            --text-muted: #5c6a7a;
            --border-color: #2a3a4d;
            --glow-cyan: 0 0 20px rgba(0, 245, 212, 0.3);
            --glow-magenta: 0 0 20px rgba(247, 37, 133, 0.3);
        }
        
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Sora', sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            background-image: 
                radial-gradient(ellipse at 20% 20%, rgba(0, 245, 212, 0.05) 0%, transparent 50%),
                radial-gradient(ellipse at 80% 80%, rgba(247, 37, 133, 0.05) 0%, transparent 50%),
                linear-gradient(180deg, var(--bg-primary) 0%, var(--bg-secondary) 100%);
        }
        
        .container {
            max-width: 1600px;
            margin: 0 auto;
            padding: 2rem;
        }
        
        header {
            text-align: center;
            margin-bottom: 3rem;
            padding: 2rem 0;
            position: relative;
        }
        
        header::before {
            content: '';
            position: absolute;
            top: 0;
            left: 50%;
            transform: translateX(-50%);
            width: 200px;
            height: 2px;
            background: linear-gradient(90deg, transparent, var(--accent-cyan), transparent);
        }
        
        h1 {
            font-size: 2.5rem;
            font-weight: 700;
            background: linear-gradient(135deg, var(--accent-cyan) 0%, var(--accent-magenta) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 0.5rem;
            letter-spacing: -0.02em;
        }
        
        .subtitle {
            color: var(--text-secondary);
            font-size: 1.1rem;
            font-weight: 300;
        }
        
        .controls {
            display: flex;
            gap: 1.5rem;
            margin-bottom: 2.5rem;
            flex-wrap: wrap;
            justify-content: center;
            align-items: center;
        }
        
        .control-group {
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
        }
        
        .control-group label {
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            color: var(--text-muted);
            font-weight: 500;
        }
        
        select {
            background: var(--bg-tertiary);
            border: 1px solid var(--border-color);
            color: var(--text-primary);
            padding: 0.75rem 1.25rem;
            font-size: 0.95rem;
            font-family: 'JetBrains Mono', monospace;
            border-radius: 8px;
            cursor: pointer;
            min-width: 220px;
            appearance: none;
            background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' fill='%239aa5b1' viewBox='0 0 16 16'%3E%3Cpath d='M8 11L3 6h10l-5 5z'/%3E%3C/svg%3E");
            background-repeat: no-repeat;
            background-position: right 1rem center;
            transition: all 0.2s ease;
        }
        
        select:hover {
            border-color: var(--accent-cyan);
            box-shadow: var(--glow-cyan);
        }
        
        select:focus {
            outline: none;
            border-color: var(--accent-cyan);
            box-shadow: var(--glow-cyan);
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1.25rem;
            margin-bottom: 2.5rem;
        }
        
        .stat-card {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 1.5rem;
            position: relative;
            overflow: hidden;
            transition: all 0.3s ease;
        }
        
        .stat-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 3px;
        }
        
        .stat-card.cyan::before { background: var(--accent-cyan); }
        .stat-card.magenta::before { background: var(--accent-magenta); }
        .stat-card.yellow::before { background: var(--accent-yellow); }
        .stat-card.orange::before { background: var(--accent-orange); }
        .stat-card.purple::before { background: var(--accent-purple); }
        
        .stat-card:hover {
            transform: translateY(-4px);
            border-color: var(--text-muted);
        }
        
        .stat-label {
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--text-muted);
            margin-bottom: 0.5rem;
        }
        
        .stat-value {
            font-family: 'JetBrains Mono', monospace;
            font-size: 1.8rem;
            font-weight: 600;
        }
        
        .stat-card.cyan .stat-value { color: var(--accent-cyan); }
        .stat-card.magenta .stat-value { color: var(--accent-magenta); }
        .stat-card.yellow .stat-value { color: var(--accent-yellow); }
        .stat-card.orange .stat-value { color: var(--accent-orange); }
        .stat-card.purple .stat-value { color: var(--accent-purple); }
        
        .stat-unit {
            font-size: 0.85rem;
            color: var(--text-secondary);
            margin-left: 0.25rem;
        }
        
        .charts-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 1.5rem;
        }
        
        @media (max-width: 1200px) {
            .charts-grid {
                grid-template-columns: 1fr;
            }
        }
        
        .chart-container {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 1.5rem;
            position: relative;
        }
        
        .chart-title {
            font-size: 1rem;
            font-weight: 600;
            margin-bottom: 1rem;
            color: var(--text-primary);
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        .chart-title .dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
        }
        
        .chart-title .dot.cyan { background: var(--accent-cyan); }
        .chart-title .dot.magenta { background: var(--accent-magenta); }
        .chart-title .dot.yellow { background: var(--accent-yellow); }
        .chart-title .dot.orange { background: var(--accent-orange); }
        .chart-title .dot.purple { background: var(--accent-purple); }
        
        .chart-wrapper {
            height: 320px;
            position: relative;
        }
        
        .full-width {
            grid-column: 1 / -1;
        }
        
        .full-width .chart-wrapper {
            height: 400px;
        }
        
        .loading {
            display: flex;
            align-items: center;
            justify-content: center;
            height: 200px;
            color: var(--text-muted);
        }
        
        .loading-spinner {
            width: 40px;
            height: 40px;
            border: 3px solid var(--border-color);
            border-top-color: var(--accent-cyan);
            border-radius: 50%;
            animation: spin 1s linear infinite;
        }
        
        @keyframes spin {
            to { transform: rotate(360deg); }
        }
        
        .country-badge {
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            background: var(--bg-tertiary);
            padding: 0.5rem 1rem;
            border-radius: 20px;
            font-size: 0.85rem;
            color: var(--text-secondary);
            margin-top: 0.5rem;
        }
        
        .country-badge .flag {
            font-size: 1.2rem;
        }
        
        footer {
            text-align: center;
            padding: 2rem;
            color: var(--text-muted);
            font-size: 0.85rem;
            margin-top: 2rem;
            border-top: 1px solid var(--border-color);
        }
        
        footer a {
            color: var(--accent-cyan);
            text-decoration: none;
        }
        
        .data-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 1rem;
            font-size: 0.9rem;
        }
        
        .data-table th,
        .data-table td {
            padding: 0.75rem;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
        }
        
        .data-table th {
            font-weight: 500;
            color: var(--text-muted);
            text-transform: uppercase;
            font-size: 0.75rem;
            letter-spacing: 0.05em;
        }
        
        .data-table td {
            font-family: 'JetBrains Mono', monospace;
        }
        
        .data-table tr:hover {
            background: var(--bg-tertiary);
        }
        
        .metric-toggle {
            display: flex;
            gap: 0.5rem;
            flex-wrap: wrap;
            justify-content: center;
            margin-bottom: 1.5rem;
        }
        
        .toggle-btn {
            background: var(--bg-tertiary);
            border: 1px solid var(--border-color);
            color: var(--text-secondary);
            padding: 0.5rem 1rem;
            font-size: 0.85rem;
            font-family: 'Sora', sans-serif;
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.2s ease;
        }
        
        .toggle-btn:hover {
            border-color: var(--text-muted);
        }
        
        .toggle-btn.active {
            background: var(--accent-cyan);
            color: var(--bg-primary);
            border-color: var(--accent-cyan);
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>⚡ European Energy Market</h1>
            <p class="subtitle">Solar Capture Prices & Negative Price Hours Analysis • 2025</p>
        </header>
        
        <div class="controls">
            <div class="control-group">
                <label>Bidding Zone</label>
                <select id="countrySelect">
                    <option value="all">All Zones (Comparison)</option>
                </select>
            </div>
            <div class="control-group">
                <label>Time Period</label>
                <select id="monthSelect">
                    <option value="all">Full Year 2025</option>
                    <option value="1">January</option>
                    <option value="2">February</option>
                    <option value="3">March</option>
                    <option value="4">April</option>
                    <option value="5">May</option>
                    <option value="6">June</option>
                    <option value="7">July</option>
                    <option value="8">August</option>
                    <option value="9">September</option>
                    <option value="10">October</option>
                    <option value="11">November</option>
                </select>
            </div>
        </div>
        
        <div class="stats-grid" id="statsGrid">
            <div class="stat-card cyan">
                <div class="stat-label">Negative Price Hours</div>
                <div class="stat-value" id="statNegHours">—<span class="stat-unit">hrs</span></div>
            </div>
            <div class="stat-card magenta">
                <div class="stat-label">Avg Market Price</div>
                <div class="stat-value" id="statAvgPrice">—<span class="stat-unit">€/MWh</span></div>
            </div>
            <div class="stat-card yellow">
                <div class="stat-label">Solar Capture Price</div>
                <div class="stat-value" id="statCapture">—<span class="stat-unit">€/MWh</span></div>
            </div>
            <div class="stat-card orange">
                <div class="stat-label">Capture Price (Floor 0)</div>
                <div class="stat-value" id="statCaptureFloor">—<span class="stat-unit">€/MWh</span></div>
            </div>
            <div class="stat-card purple">
                <div class="stat-label">Solar at Neg. Prices</div>
                <div class="stat-value" id="statSolarNeg">—<span class="stat-unit">%</span></div>
            </div>
        </div>
        
        <div class="charts-grid">
            <div class="chart-container">
                <div class="chart-title"><span class="dot cyan"></span>Negative Price Hours</div>
                <div class="chart-wrapper">
                    <canvas id="negHoursChart"></canvas>
                </div>
            </div>
            
            <div class="chart-container">
                <div class="chart-title"><span class="dot magenta"></span>Average Market Price</div>
                <div class="chart-wrapper">
                    <canvas id="avgPriceChart"></canvas>
                </div>
            </div>
            
            <div class="chart-container">
                <div class="chart-title"><span class="dot yellow"></span>Solar Capture Price</div>
                <div class="chart-wrapper">
                    <canvas id="capturePriceChart"></canvas>
                </div>
            </div>
            
            <div class="chart-container">
                <div class="chart-title"><span class="dot orange"></span>Capture Price (Floor at 0)</div>
                <div class="chart-wrapper">
                    <canvas id="captureFloor0Chart"></canvas>
                </div>
            </div>
            
            <div class="chart-container full-width">
                <div class="chart-title"><span class="dot purple"></span>Solar Volume at Negative Prices (%)</div>
                <div class="chart-wrapper">
                    <canvas id="solarNegChart"></canvas>
                </div>
            </div>
            
            <div class="chart-container full-width">
                <div class="chart-title"><span class="dot cyan"></span>Price Comparison by Zone</div>
                <div class="chart-wrapper">
                    <canvas id="comparisonChart"></canvas>
                </div>
            </div>
        </div>
        
        <footer>
            Data source: ENTSO-E Transparency Platform • Dashboard built with DuckDB & Chart.js
        </footer>
    </div>
    
    <script>
        const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const CHART_COLORS = {
            cyan: 'rgba(0, 245, 212, 1)',
            cyanBg: 'rgba(0, 245, 212, 0.2)',
            magenta: 'rgba(247, 37, 133, 1)',
            magentaBg: 'rgba(247, 37, 133, 0.2)',
            yellow: 'rgba(254, 228, 64, 1)',
            yellowBg: 'rgba(254, 228, 64, 0.2)',
            orange: 'rgba(255, 107, 53, 1)',
            orangeBg: 'rgba(255, 107, 53, 0.2)',
            purple: 'rgba(157, 78, 221, 1)',
            purpleBg: 'rgba(157, 78, 221, 0.2)',
        };
        
        let allData = [];
        let dailyData = [];
        let charts = {};
        
        const chartOptions = {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false,
                },
                tooltip: {
                    backgroundColor: 'rgba(17, 24, 32, 0.95)',
                    titleColor: '#e8eaed',
                    bodyColor: '#9aa5b1',
                    borderColor: '#2a3a4d',
                    borderWidth: 1,
                    padding: 12,
                    titleFont: { family: 'Sora', weight: 600 },
                    bodyFont: { family: 'JetBrains Mono' },
                }
            },
            scales: {
                x: {
                    grid: { color: 'rgba(42, 58, 77, 0.5)', drawBorder: false },
                    ticks: { color: '#5c6a7a', font: { family: 'Sora', size: 11 } }
                },
                y: {
                    grid: { color: 'rgba(42, 58, 77, 0.5)', drawBorder: false },
                    ticks: { color: '#5c6a7a', font: { family: 'JetBrains Mono', size: 11 } }
                }
            }
        };
        
        async function fetchData() {
            const response = await fetch('/api/data');
            const json = await response.json();
            return json.data;
        }
        
        async function fetchDailyData() {
            const response = await fetch('/api/daily-data');
            const json = await response.json();
            return json.data;
        }
        
        async function fetchCountries() {
            const response = await fetch('/api/countries');
            const json = await response.json();
            return json.countries;
        }
        
        function initCharts() {
            const ctxNeg = document.getElementById('negHoursChart').getContext('2d');
            const ctxAvg = document.getElementById('avgPriceChart').getContext('2d');
            const ctxCapture = document.getElementById('capturePriceChart').getContext('2d');
            const ctxFloor = document.getElementById('captureFloor0Chart').getContext('2d');
            const ctxSolarNeg = document.getElementById('solarNegChart').getContext('2d');
            const ctxComparison = document.getElementById('comparisonChart').getContext('2d');
            
            charts.negHours = new Chart(ctxNeg, {
                type: 'bar',
                data: { labels: [], datasets: [] },
                options: { ...chartOptions, plugins: { ...chartOptions.plugins, legend: { display: false } } }
            });
            
            charts.avgPrice = new Chart(ctxAvg, {
                type: 'bar',
                data: { labels: [], datasets: [] },
                options: { ...chartOptions, plugins: { ...chartOptions.plugins, legend: { display: false } } }
            });
            
            charts.capturePrice = new Chart(ctxCapture, {
                type: 'bar',
                data: { labels: [], datasets: [] },
                options: { ...chartOptions, plugins: { ...chartOptions.plugins, legend: { display: false } } }
            });
            
            charts.captureFloor0 = new Chart(ctxFloor, {
                type: 'bar',
                data: { labels: [], datasets: [] },
                options: { ...chartOptions, plugins: { ...chartOptions.plugins, legend: { display: false } } }
            });
            
            charts.solarNeg = new Chart(ctxSolarNeg, {
                type: 'bar',
                data: { labels: [], datasets: [] },
                options: {
                    ...chartOptions,
                    plugins: {
                        ...chartOptions.plugins,
                        legend: { display: false }
                    },
                    scales: {
                        ...chartOptions.scales,
                        y: { 
                            ...chartOptions.scales.y, 
                            max: 100,
                            ticks: { 
                                ...chartOptions.scales.y.ticks,
                                callback: function(value) { return value + '%'; }
                            }
                        },
                        x: { ...chartOptions.scales.x, ticks: { ...chartOptions.scales.x.ticks, maxRotation: 45, minRotation: 45 } }
                    }
                }
            });
            
            charts.comparison = new Chart(ctxComparison, {
                type: 'bar',
                data: { labels: [], datasets: [] },
                options: {
                    ...chartOptions,
                    plugins: {
                        ...chartOptions.plugins,
                        legend: {
                            display: true,
                            position: 'top',
                            labels: { color: '#9aa5b1', font: { family: 'Sora', size: 11 }, boxWidth: 12, padding: 15 }
                        }
                    },
                    scales: {
                        ...chartOptions.scales,
                        x: { ...chartOptions.scales.x, ticks: { ...chartOptions.scales.x.ticks, maxRotation: 45, minRotation: 45 } }
                    }
                }
            });
        }
        
        function updateCharts(country, month) {
            let filtered = allData;
            
            if (country !== 'all') {
                filtered = filtered.filter(d => d.country === country);
            }
            if (month !== 'all') {
                filtered = filtered.filter(d => d.month === parseInt(month));
            }
            
            // Update stats
            const totalNegHours = filtered.reduce((sum, d) => sum + d.neg_hours, 0);
            const avgMarketPrice = filtered.length > 0 ? (filtered.reduce((sum, d) => sum + d.avg_market_price, 0) / filtered.length) : 0;
            const avgCapture = filtered.length > 0 ? (filtered.reduce((sum, d) => sum + d.capture_price, 0) / filtered.length) : 0;
            const avgCaptureFloor = filtered.length > 0 ? (filtered.reduce((sum, d) => sum + d.capture_price_floor0, 0) / filtered.length) : 0;
            const avgSolarNeg = filtered.length > 0 ? (filtered.reduce((sum, d) => sum + d.solar_at_neg_price_pct, 0) / filtered.length) : 0;
            
            document.getElementById('statNegHours').innerHTML = `${totalNegHours.toFixed(1)}<span class="stat-unit">hrs</span>`;
            document.getElementById('statAvgPrice').innerHTML = `${avgMarketPrice.toFixed(2)}<span class="stat-unit">€/MWh</span>`;
            document.getElementById('statCapture').innerHTML = `${avgCapture.toFixed(2)}<span class="stat-unit">€/MWh</span>`;
            document.getElementById('statCaptureFloor').innerHTML = `${avgCaptureFloor.toFixed(2)}<span class="stat-unit">€/MWh</span>`;
            document.getElementById('statSolarNeg').innerHTML = `${avgSolarNeg.toFixed(1)}<span class="stat-unit">%</span>`;
            
            if (country !== 'all' && month !== 'all') {
                // Single country + specific month view - show DAILY data
                const dailyCountryData = dailyData
                    .filter(d => d.country === country && d.month === parseInt(month))
                    .sort((a, b) => a.day - b.day);
                const labels = dailyCountryData.map(d => d.day.toString());
                
                charts.negHours.data = {
                    labels,
                    datasets: [{
                        data: dailyCountryData.map(d => d.neg_hours),
                        backgroundColor: CHART_COLORS.cyanBg,
                        borderColor: CHART_COLORS.cyan,
                        borderWidth: 2,
                        borderRadius: 4,
                    }]
                };
                
                charts.avgPrice.data = {
                    labels,
                    datasets: [{
                        data: dailyCountryData.map(d => d.avg_market_price),
                        backgroundColor: CHART_COLORS.magentaBg,
                        borderColor: CHART_COLORS.magenta,
                        borderWidth: 2,
                        borderRadius: 4,
                    }]
                };
                
                charts.capturePrice.data = {
                    labels,
                    datasets: [{
                        data: dailyCountryData.map(d => d.capture_price),
                        backgroundColor: CHART_COLORS.yellowBg,
                        borderColor: CHART_COLORS.yellow,
                        borderWidth: 2,
                        borderRadius: 4,
                    }]
                };
                
                charts.captureFloor0.data = {
                    labels,
                    datasets: [{
                        data: dailyCountryData.map(d => d.capture_price_floor0),
                        backgroundColor: CHART_COLORS.orangeBg,
                        borderColor: CHART_COLORS.orange,
                        borderWidth: 2,
                        borderRadius: 4,
                    }]
                };
                
                charts.solarNeg.data = {
                    labels,
                    datasets: [{
                        data: dailyCountryData.map(d => d.solar_at_neg_price_pct),
                        backgroundColor: CHART_COLORS.purpleBg,
                        borderColor: CHART_COLORS.purple,
                        borderWidth: 2,
                        borderRadius: 4,
                    }]
                };
                
            } else if (country !== 'all') {
                // Single country view (full year) - show MONTHLY data
                const countryData = allData.filter(d => d.country === country);
                const labels = countryData.map(d => MONTHS[d.month - 1]);
                
                charts.negHours.data = {
                    labels,
                    datasets: [{
                        data: countryData.map(d => d.neg_hours),
                        backgroundColor: CHART_COLORS.cyanBg,
                        borderColor: CHART_COLORS.cyan,
                        borderWidth: 2,
                        borderRadius: 4,
                    }]
                };
                
                charts.avgPrice.data = {
                    labels,
                    datasets: [{
                        data: countryData.map(d => d.avg_market_price),
                        backgroundColor: CHART_COLORS.magentaBg,
                        borderColor: CHART_COLORS.magenta,
                        borderWidth: 2,
                        borderRadius: 4,
                    }]
                };
                
                charts.capturePrice.data = {
                    labels,
                    datasets: [{
                        data: countryData.map(d => d.capture_price),
                        backgroundColor: CHART_COLORS.yellowBg,
                        borderColor: CHART_COLORS.yellow,
                        borderWidth: 2,
                        borderRadius: 4,
                    }]
                };
                
                charts.captureFloor0.data = {
                    labels,
                    datasets: [{
                        data: countryData.map(d => d.capture_price_floor0),
                        backgroundColor: CHART_COLORS.orangeBg,
                        borderColor: CHART_COLORS.orange,
                        borderWidth: 2,
                        borderRadius: 4,
                    }]
                };
                
                charts.solarNeg.data = {
                    labels,
                    datasets: [{
                        data: countryData.map(d => d.solar_at_neg_price_pct),
                        backgroundColor: CHART_COLORS.purpleBg,
                        borderColor: CHART_COLORS.purple,
                        borderWidth: 2,
                        borderRadius: 4,
                    }]
                };
                
            } else {
                // All countries view - aggregate or filter by month
                let aggregated;
                if (month !== 'all') {
                    aggregated = allData.filter(d => d.month === parseInt(month));
                } else {
                    // Aggregate by country across all months
                    const byCountry = {};
                    allData.forEach(d => {
                        if (!byCountry[d.country]) {
                            byCountry[d.country] = { neg_hours: 0, avg_market_price: 0, capture_price: 0, capture_price_floor0: 0, solar_at_neg_price_pct: 0, count: 0 };
                        }
                        byCountry[d.country].neg_hours += d.neg_hours;
                        byCountry[d.country].avg_market_price += d.avg_market_price;
                        byCountry[d.country].capture_price += d.capture_price;
                        byCountry[d.country].capture_price_floor0 += d.capture_price_floor0;
                        byCountry[d.country].solar_at_neg_price_pct += d.solar_at_neg_price_pct;
                        byCountry[d.country].count++;
                    });
                    aggregated = Object.entries(byCountry).map(([country, data]) => ({
                        country,
                        neg_hours: data.neg_hours,
                        avg_market_price: data.avg_market_price / data.count,
                        capture_price: data.capture_price / data.count,
                        capture_price_floor0: data.capture_price_floor0 / data.count,
                        solar_at_neg_price_pct: data.solar_at_neg_price_pct / data.count,
                    }));
                }
                
                // Sort by negative hours descending
                aggregated.sort((a, b) => b.neg_hours - a.neg_hours);
                const top15 = aggregated.slice(0, 15);
                const labels = top15.map(d => d.country.replace(/\\s*\\([^)]+\\)/, ''));
                
                charts.negHours.data = {
                    labels,
                    datasets: [{
                        data: top15.map(d => d.neg_hours),
                        backgroundColor: CHART_COLORS.cyanBg,
                        borderColor: CHART_COLORS.cyan,
                        borderWidth: 2,
                        borderRadius: 4,
                    }]
                };
                
                // Sort by avg price for that chart
                const byAvgPrice = [...aggregated].sort((a, b) => b.avg_market_price - a.avg_market_price).slice(0, 15);
                charts.avgPrice.data = {
                    labels: byAvgPrice.map(d => d.country.replace(/\\s*\\([^)]+\\)/, '')),
                    datasets: [{
                        data: byAvgPrice.map(d => d.avg_market_price),
                        backgroundColor: CHART_COLORS.magentaBg,
                        borderColor: CHART_COLORS.magenta,
                        borderWidth: 2,
                        borderRadius: 4,
                    }]
                };
                
                // Sort by capture price for that chart
                const byCapture = [...aggregated].sort((a, b) => b.capture_price - a.capture_price).slice(0, 15);
                charts.capturePrice.data = {
                    labels: byCapture.map(d => d.country.replace(/\\s*\\([^)]+\\)/, '')),
                    datasets: [{
                        data: byCapture.map(d => d.capture_price),
                        backgroundColor: CHART_COLORS.yellowBg,
                        borderColor: CHART_COLORS.yellow,
                        borderWidth: 2,
                        borderRadius: 4,
                    }]
                };
                
                // Sort by capture floor for that chart
                const byCaptureFloor = [...aggregated].sort((a, b) => b.capture_price_floor0 - a.capture_price_floor0).slice(0, 15);
                charts.captureFloor0.data = {
                    labels: byCaptureFloor.map(d => d.country.replace(/\\s*\\([^)]+\\)/, '')),
                    datasets: [{
                        data: byCaptureFloor.map(d => d.capture_price_floor0),
                        backgroundColor: CHART_COLORS.orangeBg,
                        borderColor: CHART_COLORS.orange,
                        borderWidth: 2,
                        borderRadius: 4,
                    }]
                };
                
                // Update solar neg chart for all countries view
                const solarNegData = [...aggregated].sort((a, b) => b.solar_at_neg_price_pct - a.solar_at_neg_price_pct).slice(0, 15);
                charts.solarNeg.data = {
                    labels: solarNegData.map(d => d.country.replace(/\\s*\\([^)]+\\)/, '')),
                    datasets: [{
                        data: solarNegData.map(d => d.solar_at_neg_price_pct),
                        backgroundColor: CHART_COLORS.purpleBg,
                        borderColor: CHART_COLORS.purple,
                        borderWidth: 2,
                        borderRadius: 4,
                    }]
                };
            }
            
            // Update comparison chart - top 10 countries by negative hours
            let comparisonData;
            if (month !== 'all') {
                comparisonData = allData.filter(d => d.month === parseInt(month));
            } else {
                const byCountry = {};
                allData.forEach(d => {
                    if (!byCountry[d.country]) {
                        byCountry[d.country] = { neg_hours: 0, avg_market_price: 0, capture_price: 0, capture_price_floor0: 0, solar_at_neg_price_pct: 0, count: 0 };
                    }
                    byCountry[d.country].neg_hours += d.neg_hours;
                    byCountry[d.country].avg_market_price += d.avg_market_price;
                    byCountry[d.country].capture_price += d.capture_price;
                    byCountry[d.country].capture_price_floor0 += d.capture_price_floor0;
                    byCountry[d.country].solar_at_neg_price_pct += d.solar_at_neg_price_pct;
                    byCountry[d.country].count++;
                });
                comparisonData = Object.entries(byCountry).map(([country, data]) => ({
                    country,
                    neg_hours: data.neg_hours,
                    avg_market_price: data.avg_market_price / data.count,
                    capture_price: data.capture_price / data.count,
                    capture_price_floor0: data.capture_price_floor0 / data.count,
                    solar_at_neg_price_pct: data.solar_at_neg_price_pct / data.count,
                }));
            }
            
            comparisonData.sort((a, b) => b.neg_hours - a.neg_hours);
            const top10 = comparisonData.slice(0, 10);
            
            charts.comparison.data = {
                labels: top10.map(d => d.country.replace(/\\s*\\([^)]+\\)/, '')),
                datasets: [
                    {
                        label: 'Avg Market Price (€/MWh)',
                        data: top10.map(d => d.avg_market_price),
                        backgroundColor: CHART_COLORS.magentaBg,
                        borderColor: CHART_COLORS.magenta,
                        borderWidth: 2,
                        borderRadius: 4,
                    },
                    {
                        label: 'Capture Price (€/MWh)',
                        data: top10.map(d => d.capture_price),
                        backgroundColor: CHART_COLORS.yellowBg,
                        borderColor: CHART_COLORS.yellow,
                        borderWidth: 2,
                        borderRadius: 4,
                    },
                    {
                        label: 'Capture Price Floor 0 (€/MWh)',
                        data: top10.map(d => d.capture_price_floor0),
                        backgroundColor: CHART_COLORS.orangeBg,
                        borderColor: CHART_COLORS.orange,
                        borderWidth: 2,
                        borderRadius: 4,
                    },
                ]
            };
            
            // Update all charts
            Object.values(charts).forEach(chart => chart.update());
        }
        
        async function init() {
            initCharts();
            
            // Fetch data (monthly and daily)
            allData = await fetchData();
            dailyData = await fetchDailyData();
            const countries = await fetchCountries();
            
            // Populate country select
            const countrySelect = document.getElementById('countrySelect');
            countries.forEach(country => {
                const option = document.createElement('option');
                option.value = country;
                option.textContent = country;
                countrySelect.appendChild(option);
            });
            
            // Event listeners
            countrySelect.addEventListener('change', () => {
                updateCharts(countrySelect.value, document.getElementById('monthSelect').value);
            });
            
            document.getElementById('monthSelect').addEventListener('change', () => {
                updateCharts(countrySelect.value, document.getElementById('monthSelect').value);
            });
            
            // Initial render
            updateCharts('all', 'all');
        }
        
        init();
    </script>
</body>
</html>
"""
    return html_content


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

