"""
European Energy Market Dashboard
Full featured with capture prices and summary tables
"""

import os
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, FileResponse
import mysql.connector

# Load .env file if it exists (for local development)
try:
    env_file = Path(__file__).parent / '.env'
    if env_file.exists() and env_file.is_file():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ.setdefault(key.strip(), value.strip())
except (PermissionError, IOError):
    # Silently ignore if we can't read .env (e.g., in restricted environments)
    # Environment variables should be set another way (export, Cloud Run, etc.)
    pass

app = FastAPI(title="European Energy Market Dashboard")

# Database configuration (set via environment variables in Cloud Run)
# These must be set as environment variables - no defaults for security
def get_db_config():
    """Get database configuration from environment variables"""
    required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD']
    missing = [v for v in required_vars if not os.environ.get(v)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}. "
                        f"Please set them in .env file or export them.")
    
    return {
        'host': os.environ['DB_HOST'],
        'port': int(os.environ.get('DB_PORT', '3306')),
        'user': os.environ['DB_USER'],
        'password': os.environ['DB_PASSWORD'],
        'database': os.environ.get('DB_NAME', 'energy_market')
    }

# Initialize DB config (will raise error if vars not set)
try:
    _db_config = get_db_config()
    DB_HOST = _db_config['host']
    DB_PORT = _db_config['port']
    DB_USER = _db_config['user']
    DB_PASSWORD = _db_config['password']
    DB_NAME = _db_config['database']
except ValueError as e:
    # Store error to show helpful message
    _db_config_error = str(e)
    DB_HOST = DB_PORT = DB_USER = DB_PASSWORD = DB_NAME = None


def get_db_connection():
    if not all([DB_HOST, DB_USER, DB_PASSWORD]):
        raise RuntimeError(
            "Database configuration not set. Please set DB_HOST, DB_USER, and DB_PASSWORD environment variables. "
            "For local development, create a .env file with these variables."
        )
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, use_pure=True, connection_timeout=30
    )


@app.get("/health")
def health():
    try:
        conn = get_db_connection()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.get("/debug/static")
def debug_static():
    """Debug endpoint to check if static files exist"""
    import os
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    files = []
    if os.path.exists(static_dir):
        files = os.listdir(static_dir)
    return {
        "static_dir_exists": os.path.exists(static_dir),
        "static_dir_path": static_dir,
        "files": files,
        "profile_image_exists": os.path.exists(os.path.join(static_dir, "250509_PGB9975_1.jpg"))
    }


@app.get("/api/summary/total")
def get_summary_total():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT total_neg_hours, avg_market_price, capture_price, 
               capture_price_floor0, capture_rate, solar_at_neg_price_pct 
        FROM summary_total WHERE id = 1
    """)
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    
    return {
        "neg_hours": float(row[0] or 0),
        "avg_market_price": float(row[1] or 0),
        "capture_price": float(row[2] or 0),
        "capture_price_floor0": float(row[3] or 0),
        "capture_rate": float(row[4] or 0),
        "solar_at_neg_price_pct": float(row[5] or 0)
    }


@app.get("/api/summary/yearly")
def get_summary_yearly():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT country, total_neg_hours, avg_market_price, capture_price,
               capture_price_floor0, capture_rate, solar_at_neg_price_pct
        FROM summary_yearly ORDER BY country
    """)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    data = [{
        "country": r[0], "neg_hours": float(r[1] or 0), "avg_market_price": float(r[2] or 0),
        "capture_price": float(r[3] or 0), "capture_price_floor0": float(r[4] or 0),
        "capture_rate": float(r[5] or 0), "solar_at_neg_price_pct": float(r[6] or 0)
    } for r in results]
    return {"data": data}


@app.get("/api/summary/years")
def get_available_years():
    """Get list of available years in the database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Try to get years from summary_monthly.year column first
    # If that column doesn't exist, extract from energy_prices DateTime
    try:
        cursor.execute("""
            SELECT DISTINCT year FROM summary_monthly ORDER BY year DESC
        """)
        years = [row[0] for row in cursor.fetchall()]
    except mysql.connector.Error:
        # If year column doesn't exist, extract from DateTime in energy_prices
        cursor.execute("""
            SELECT DISTINCT YEAR(`DateTime(UTC)`) as year 
            FROM energy_prices 
            ORDER BY year DESC
        """)
        years = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    # If no years found, return current year as default
    if not years:
        years = [datetime.now().year]
    
    return {"years": years}


@app.get("/api/summary/monthly")
def get_summary_monthly(year: int = None):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check if year column exists
    cursor.execute("SHOW COLUMNS FROM summary_monthly LIKE 'year'")
    has_year_column = cursor.fetchone() is not None
    
    if has_year_column:
        # Use year column if it exists
        if year:
            cursor.execute("""
                SELECT country, month, year, neg_hours, avg_market_price, capture_price,
                       capture_price_floor0, capture_rate, solar_at_neg_price_pct
                FROM summary_monthly 
                WHERE year = %s
                ORDER BY country, month
            """, (year,))
        else:
            # Default to current year if no year specified
            current_year = datetime.now().year
            cursor.execute("""
                SELECT country, month, year, neg_hours, avg_market_price, capture_price,
                       capture_price_floor0, capture_rate, solar_at_neg_price_pct
                FROM summary_monthly 
                WHERE year = %s
                ORDER BY country, month
            """, (current_year,))
        
        results = cursor.fetchall()
        data = [{
            "country": r[0], "month": r[1], "year": r[2], "neg_hours": float(r[3] or 0), 
            "avg_market_price": float(r[4] or 0), "capture_price": float(r[5] or 0),
            "capture_price_floor0": float(r[6] or 0), "capture_rate": float(r[7] or 0),
            "solar_at_neg_price_pct": float(r[8] or 0)
        } for r in results]
    else:
        # Fallback: no year column, return all data and assume current year
        cursor.execute("""
            SELECT country, month, neg_hours, avg_market_price, capture_price,
                   capture_price_floor0, capture_rate, solar_at_neg_price_pct
            FROM summary_monthly 
            ORDER BY country, month
        """)
        results = cursor.fetchall()
        current_year = datetime.now().year
        data = [{
            "country": r[0], "month": r[1], "year": current_year, "neg_hours": float(r[2] or 0), 
            "avg_market_price": float(r[3] or 0), "capture_price": float(r[4] or 0),
            "capture_price_floor0": float(r[5] or 0), "capture_rate": float(r[6] or 0),
            "solar_at_neg_price_pct": float(r[7] or 0)
        } for r in results]
    
    cursor.close()
    conn.close()
    return {"data": data}


@app.get("/api/hourly/day")
def get_hourly_day_data(country: str, month: int, day: int, year: int):
    """Get hourly price and solar generation data for a specific day"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Build date string for the specific day
    date_str = f"{year}-{month:02d}-{day:02d}"
    
    # Get price data for the day (Sequence 1 = Day-Ahead Auction)
    cursor.execute("""
        SELECT 
            `DateTime(UTC)`,
            `Price[Currency/MWh]`,
            ResolutionCode
        FROM energy_prices
        WHERE 
            AreaDisplayName = %s
            AND DATE(`DateTime(UTC)`) = %s
            AND `Sequence` = '1'
        ORDER BY `DateTime(UTC)`
    """, (country, date_str))
    
    price_data = cursor.fetchall()
    
    # Get solar generation data for the day
    cursor.execute("""
        SELECT 
            `DateTime(UTC)`,
            ActualGenerationOutput
        FROM generation_per_type
        WHERE 
            AreaDisplayName = %s
            AND DATE(`DateTime(UTC)`) = %s
            AND ProductionType = 'Solar'
        ORDER BY `DateTime(UTC)`
    """, (country, date_str))
    
    solar_data = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    # Convert to dictionaries with timestamps
    prices = {str(row[0]): float(row[1] or 0) for row in price_data}
    solar = {str(row[0]): float(row[1] or 0) for row in solar_data}
    
    # Get all unique timestamps and merge data
    all_timestamps = sorted(set(list(prices.keys()) + list(solar.keys())))
    
    result = []
    for ts in all_timestamps:
        result.append({
            "timestamp": ts,
            "price": prices.get(ts, 0),
            "solar_mw": solar.get(ts, 0)
        })
    
    return {"data": result, "country": country, "date": date_str}


@app.get("/api/summary/daily")
def get_summary_daily(country: str = None, month: int = None, year: int = None):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Default to current year if not specified
    if year is None:
        year = datetime.now().year
    
    # Check if year column exists
    cursor.execute("SHOW COLUMNS FROM summary_daily LIKE 'year'")
    has_year_column = cursor.fetchone() is not None
    
    if has_year_column:
        # Use year column if it exists
        if country and month:
            cursor.execute("""
                SELECT country, month, year, day, neg_hours, avg_market_price, capture_price,
                       capture_price_floor0, capture_rate, solar_at_neg_price_pct
                FROM summary_daily 
                WHERE country = %s AND month = %s AND year = %s
                ORDER BY day
            """, (country, month, year))
        else:
            cursor.execute("""
                SELECT country, month, year, day, neg_hours, avg_market_price, capture_price,
                       capture_price_floor0, capture_rate, solar_at_neg_price_pct
                FROM summary_daily 
                WHERE year = %s
                ORDER BY country, month, day
            """, (year,))
        
        results = cursor.fetchall()
        data = [{
            "country": r[0], "month": r[1], "year": r[2], "day": r[3], "neg_hours": float(r[4] or 0), 
            "avg_market_price": float(r[5] or 0), "capture_price": float(r[6] or 0),
            "capture_price_floor0": float(r[7] or 0), "capture_rate": float(r[8] or 0),
            "solar_at_neg_price_pct": float(r[9] or 0)
        } for r in results]
    else:
        # Fallback: no year column, return all data and assume current year
        if country and month:
            cursor.execute("""
                SELECT country, month, day, neg_hours, avg_market_price, capture_price,
                       capture_price_floor0, capture_rate, solar_at_neg_price_pct
                FROM summary_daily 
                WHERE country = %s AND month = %s
                ORDER BY day
            """, (country, month))
        else:
            cursor.execute("""
                SELECT country, month, day, neg_hours, avg_market_price, capture_price,
                       capture_price_floor0, capture_rate, solar_at_neg_price_pct
                FROM summary_daily 
                ORDER BY country, month, day
            """)
        
        results = cursor.fetchall()
        data = [{
            "country": r[0], "month": r[1], "year": year, "day": r[2], "neg_hours": float(r[3] or 0), 
            "avg_market_price": float(r[4] or 0), "capture_price": float(r[5] or 0),
            "capture_price_floor0": float(r[6] or 0), "capture_rate": float(r[7] or 0),
            "solar_at_neg_price_pct": float(r[8] or 0)
        } for r in results]
    
    cursor.close()
    conn.close()
    return {"data": data}


# Common styles
def get_base_styles():
    return """
        :root {
            --bg-dark: #0a0e14;
            --bg-card: #1a1f2e;
            --cyan: #00f5d4;
            --pink: #f72585;
            --yellow: #fee440;
            --orange: #ff6b35;
            --purple: #9d4edd;
            --blue: #4cc9f0;
            --text: #e8eaed;
            --text-muted: #8892a0;
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', system-ui, sans-serif;
            background: var(--bg-dark);
            color: var(--text);
            min-height: 100vh;
        }
        .container { 
            max-width: 1600px; 
            margin: 0 auto; 
            padding: 2rem; 
            width: 100%;
            overflow-x: hidden;
        }
        
        nav {
            background: var(--bg-card);
            padding: 1rem 2rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid #2a3a4d;
        }
        .logo {
            font-size: 1.8rem;
            font-weight: 700;
            background: linear-gradient(135deg, var(--cyan), var(--pink));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            text-decoration: none;
        }
        .nav-links {
            display: flex;
            gap: 2rem;
            list-style: none;
        }
        .nav-links a {
            color: var(--text-muted);
            text-decoration: none;
            font-size: 0.95rem;
            transition: color 0.2s;
        }
        .nav-links a:hover, .nav-links a.active { color: var(--cyan); }
        .dropdown { position: relative; }
        .dropdown-content {
            display: none;
            position: absolute;
            top: 100%;
            left: 0;
            background: var(--bg-card);
            border: 1px solid #2a3a4d;
            border-radius: 8px;
            min-width: 160px;
            padding: 0.5rem 0;
            z-index: 100;
        }
        .dropdown:hover .dropdown-content { display: block; }
        .dropdown-content a { display: block; padding: 0.5rem 1rem; }
        
        header { text-align: center; margin-bottom: 2rem; padding-top: 1rem; }
        h1 {
            font-size: 2.5rem;
            background: linear-gradient(135deg, var(--cyan), var(--pink));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 0.5rem;
        }
        .subtitle { color: var(--text-muted); font-size: 1.1rem; }
        
        .page-content { max-width: 800px; margin: 0 auto; padding: 2rem; }
        .page-content h2 { color: var(--cyan); margin-bottom: 1.5rem; font-size: 1.8rem; }
        .page-content p { color: var(--text-muted); line-height: 1.8; margin-bottom: 1rem; }
        .profile-section { display: flex; gap: 2rem; align-items: flex-start; margin-bottom: 2rem; }
        .profile-img { width: 180px; height: 180px; border-radius: 50%; object-fit: cover; border: 3px solid var(--cyan); }
        .social-links { display: flex; gap: 1.5rem; margin-top: 2rem; }
        .social-links a {
            display: flex; align-items: center; gap: 0.5rem;
            color: var(--text); text-decoration: none;
            padding: 0.75rem 1.5rem; background: var(--bg-card);
            border-radius: 8px; transition: all 0.2s;
        }
        .social-links a:hover { background: var(--cyan); color: var(--bg-dark); }
        .social-links svg { width: 24px; height: 24px; }
        .method-card { background: var(--bg-card); border-radius: 12px; padding: 1.5rem; margin-bottom: 1.5rem; }
        .method-card h3 { color: var(--yellow); margin-bottom: 0.75rem; }
        .formula { background: #0d1117; padding: 1rem; border-radius: 8px; font-family: monospace; color: var(--cyan); margin: 1rem 0; }
    """


def get_nav_html(active=""):
    return f"""
    <nav>
        <a href="/" class="logo">enerlyzer</a>
        <ul class="nav-links">
            <li><a href="/" class="{'active' if active=='dashboard' else ''}">Dashboard</a></li>
            <li class="dropdown">
                <a href="#" class="{'active' if active in ['project','about-me'] else ''}">About ▾</a>
                <div class="dropdown-content">
                    <a href="/about/project">The Project</a>
                    <a href="/about/me">About Me</a>
                </div>
            </li>
            <li><a href="/methodology" class="{'active' if active=='methodology' else ''}">Methodology</a></li>
            <li><a href="/contact" class="{'active' if active=='contact' else ''}">Contact</a></li>
        </ul>
    </nav>
    """


@app.get("/")
def home():
    styles = get_base_styles()
    nav = get_nav_html('dashboard')
    return HTMLResponse("""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>enerlyzer - European Energy Market Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        """ + styles + """
        .controls {
            display: flex;
            gap: 1.5rem;
            justify-content: center;
            margin-bottom: 2rem;
            flex-wrap: wrap;
        }
        .control-group { display: flex; flex-direction: column; gap: 0.5rem; }
        .control-group label { font-size: 0.75rem; text-transform: uppercase; color: var(--text-muted); }
        select {
            background: var(--bg-card);
            border: 1px solid #2a3a4d;
            color: var(--text);
            padding: 0.75rem 1rem;
            font-size: 1rem;
            border-radius: 8px;
            min-width: 200px;
            cursor: pointer;
        }
        select:hover { border-color: var(--cyan); }
        .stats {
            display: grid;
            grid-template-columns: repeat(6, 1fr);
            gap: 1rem;
            margin-bottom: 2rem;
        }
        @media (max-width: 1200px) { .stats { grid-template-columns: repeat(3, 1fr); } }
        @media (max-width: 768px) { .stats { grid-template-columns: repeat(2, 1fr); } }
        .stat-card {
            background: var(--bg-card);
            border-radius: 12px;
            padding: 1.25rem;
            border-top: 3px solid var(--cyan);
        }
        .stat-card.pink { border-top-color: var(--pink); }
        .stat-card.yellow { border-top-color: var(--yellow); }
        .stat-card.orange { border-top-color: var(--orange); }
        .stat-card.purple { border-top-color: var(--purple); }
        .stat-card.blue { border-top-color: var(--blue); }
        .stat-label { font-size: 0.7rem; color: var(--text-muted); text-transform: uppercase; }
        .stat-value { font-size: 1.5rem; font-weight: 700; margin-top: 0.3rem; }
        .stat-card:nth-child(1) .stat-value { color: var(--cyan); }
        .stat-card:nth-child(2) .stat-value { color: var(--pink); }
        .stat-card:nth-child(3) .stat-value { color: var(--yellow); }
        .stat-card:nth-child(4) .stat-value { color: var(--orange); }
        .stat-card:nth-child(5) .stat-value { color: var(--purple); }
        .stat-card:nth-child(6) .stat-value { color: var(--blue); }
        .charts {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 1.5rem;
            /* Ensure grid items don't overflow */
            min-width: 0;
        }
        @media (max-width: 1400px) { .charts { grid-template-columns: repeat(2, 1fr); } }
        @media (max-width: 768px) { .charts { grid-template-columns: 1fr; } }
        .chart-card {
            background: var(--bg-card);
            border-radius: 12px;
            padding: 1.5rem;
            /* Ensure chart-card doesn't expand */
            min-width: 0;
            overflow: hidden;
        }
        .chart-title { font-size: 0.9rem; margin-bottom: 0.75rem; color: var(--text-muted); }
        .chart-container { 
            height: 250px; 
            width: 100% !important;
            min-width: 300px;
            max-width: 100% !important;
            overflow-x: scroll !important;
            overflow-y: hidden !important;
            position: relative;
            box-sizing: border-box;
            /* Prevent container from expanding beyond its parent */
            display: block;
            /* Force container to stay within bounds */
            min-width: 0;
        }
        .chart-container canvas {
            display: block;
            /* Prevent canvas from affecting container width */
            flex-shrink: 0;
            /* Canvas can be wider than container */
            max-width: none !important;
            min-width: fit-content;
            /* Ensure canvas doesn't force container to expand */
            position: relative;
        }
        /* Enable horizontal scrolling for charts with many bars */
        .chart-container::-webkit-scrollbar {
            height: 8px;
        }
        .chart-container::-webkit-scrollbar-track {
            background: var(--bg-dark);
            border-radius: 4px;
        }
        .chart-container::-webkit-scrollbar-thumb {
            background: var(--cyan);
            border-radius: 4px;
        }
        .chart-container::-webkit-scrollbar-thumb:hover {
            background: var(--pink);
        }
    </style>
</head>
<body>
    """ + nav + """
    <div class="container">
        <header>
            <h1>enerlyzer</h1>
            <p class="subtitle">European Energy Market Dashboard • Solar Capture Prices & Negative Hours • """ + str(datetime.now().year) + """</p>
        </header>
        
        <div class="controls">
            <div class="control-group">
                <label>Bidding Zone</label>
                <select id="zoneSelect">
                    <option value="all">All Bidding Zones</option>
                    <option value="Austria (AT)">Austria (AT)</option>
                    <option value="Belgium (BE)">Belgium (BE)</option>
                    <option value="Bulgaria (BG)">Bulgaria (BG)</option>
                    <option value="Croatia (HR)">Croatia (HR)</option>
                    <option value="Czech Republic (CZ)">Czech Republic (CZ)</option>
                    <option value="DE-LU">DE-LU (Germany-Luxembourg)</option>
                    <option value="DK1">DK1 (Denmark West)</option>
                    <option value="DK2">DK2 (Denmark East)</option>
                    <option value="Estonia (EE)">Estonia (EE)</option>
                    <option value="Finland (FI)">Finland (FI)</option>
                    <option value="France (FR)">France (FR)</option>
                    <option value="Greece (GR)">Greece (GR)</option>
                    <option value="Hungary (HU)">Hungary (HU)</option>
                    <option value="IE(SEM)">Ireland (SEM)</option>
                    <option value="IT-Calabria">Italy - Calabria</option>
                    <option value="IT-Centre-North">Italy - Centre-North</option>
                    <option value="IT-Centre-South">Italy - Centre-South</option>
                    <option value="IT-North">Italy - North</option>
                    <option value="IT-Sardinia">Italy - Sardinia</option>
                    <option value="IT-Sicily">Italy - Sicily</option>
                    <option value="IT-South">Italy - South</option>
                    <option value="Latvia (LV)">Latvia (LV)</option>
                    <option value="Lithuania (LT)">Lithuania (LT)</option>
                    <option value="Netherlands (NL)">Netherlands (NL)</option>
                    <option value="NO1">NO1 (Norway South-East)</option>
                    <option value="NO2">NO2 (Norway South-West)</option>
                    <option value="NO3">NO3 (Norway Central)</option>
                    <option value="NO4">NO4 (Norway North)</option>
                    <option value="NO5">NO5 (Norway West)</option>
                    <option value="Poland (PL)">Poland (PL)</option>
                    <option value="Portugal (PT)">Portugal (PT)</option>
                    <option value="Romania (RO)">Romania (RO)</option>
                    <option value="SE1">SE1 (Sweden North)</option>
                    <option value="SE2">SE2 (Sweden Central-North)</option>
                    <option value="SE3">SE3 (Sweden Central-South)</option>
                    <option value="SE4">SE4 (Sweden South)</option>
                    <option value="Serbia (RS)">Serbia (RS)</option>
                    <option value="Slovakia (SK)">Slovakia (SK)</option>
                    <option value="Slovenia (SI)">Slovenia (SI)</option>
                    <option value="Spain (ES)">Spain (ES)</option>
                    <option value="Switzerland (CH)">Switzerland (CH)</option>
                </select>
            </div>
            <div class="control-group">
                <label>Year</label>
                <select id="yearSelect">
                    <option value=""" + str(datetime.now().year) + """>""" + str(datetime.now().year) + """</option>
                </select>
            </div>
            <div class="control-group">
                <label>Time Period</label>
                <select id="monthSelect">
                    <option value="all">Full Year</option>
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
                    <option value="12">December</option>
                </select>
            </div>
        </div>
        
        <div class="stats">
            <div class="stat-card"><div class="stat-label">Negative Price Hours</div><div class="stat-value" id="val1">—</div></div>
            <div class="stat-card pink"><div class="stat-label">Avg Market Price</div><div class="stat-value" id="val2">—</div></div>
            <div class="stat-card yellow"><div class="stat-label">Capture Price</div><div class="stat-value" id="val3">—</div></div>
            <div class="stat-card orange"><div class="stat-label">Capture (Floor €0)</div><div class="stat-value" id="val4">—</div></div>
            <div class="stat-card purple"><div class="stat-label">Capture Rate</div><div class="stat-value" id="val5">—</div></div>
            <div class="stat-card blue"><div class="stat-label">Solar @ Neg Price</div><div class="stat-value" id="val6">—</div></div>
        </div>
        
        <div class="charts">
            <div class="chart-card"><div class="chart-title">Negative Price Hours</div><div class="chart-container"><canvas id="chart1"></canvas></div></div>
            <div class="chart-card"><div class="chart-title">Avg Market Price (€/MWh)</div><div class="chart-container"><canvas id="chart2"></canvas></div></div>
            <div class="chart-card"><div class="chart-title">Capture Price (€/MWh)</div><div class="chart-container"><canvas id="chart3"></canvas></div></div>
            <div class="chart-card"><div class="chart-title">Capture Price Floor €0 (€/MWh)</div><div class="chart-container"><canvas id="chart4"></canvas></div></div>
            <div class="chart-card"><div class="chart-title">Capture Rate (%)</div><div class="chart-container"><canvas id="chart5"></canvas></div></div>
            <div class="chart-card"><div class="chart-title">Solar Volume @ Neg Price (%)</div><div class="chart-container"><canvas id="chart6"></canvas></div></div>
                </div>
            </div>
            
    <!-- Modal for hourly day view -->
    <div id="dayModal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2 class="modal-title" id="modalTitle">Day View</h2>
                <button class="close-modal" onclick="closeDayModal()">&times;</button>
            </div>
            <div class="modal-chart-container">
                <canvas id="dayChart"></canvas>
            </div>
        </div>
    </div>
            
    <script>
        let yearlyData = [], monthlyData = [], totalData = {};
        let charts = {};
        const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        const COLORS = ['#00f5d4','#f72585','#fee440','#ff6b35','#9d4edd','#4cc9f0'];
        
        async function loadData() {
            // Load available years and populate year selector
            const yearsRes = await fetch('/api/summary/years');
            const yearsData = await yearsRes.json();
            const yearSelect = document.getElementById('yearSelect');
            yearSelect.innerHTML = '';
            yearsData.years.forEach(year => {
                const option = document.createElement('option');
                option.value = year;
                option.textContent = year;
                if (year === """ + str(datetime.now().year) + """) {
                    option.selected = true;
                }
                yearSelect.appendChild(option);
            });
            
            const year = document.getElementById('yearSelect').value;
            const [t, y, m] = await Promise.all([
                fetch('/api/summary/total').then(r => r.json()),
                fetch('/api/summary/yearly').then(r => r.json()),
                fetch(`/api/summary/monthly?year=${year}`).then(r => r.json())
            ]);
            totalData = t; yearlyData = y.data; monthlyData = m.data;
            updateDisplay();
        }
        
        async function updateDisplay() {
            const zone = document.getElementById('zoneSelect').value;
            const month = document.getElementById('monthSelect').value;
            const year = document.getElementById('yearSelect').value;
            let d, labels, datasets;
            
            // Reload monthly and yearly data
            const [mRes, yRes] = await Promise.all([
                fetch(`/api/summary/monthly?year=${year}`),
                fetch('/api/summary/yearly')
            ]);
            monthlyData = (await mRes.json()).data;
            yearlyData = (await yRes.json()).data;
            
            if (zone === 'all' && month === 'all') {
                d = totalData;
                // Sort each dataset by its own metric (descending)
                const allData = [...yearlyData];
                const sortedByNegHours = [...allData].sort((a,b) => b.neg_hours - a.neg_hours);
                const sortedByAvgPrice = [...allData].sort((a,b) => b.avg_market_price - a.avg_market_price);
                const sortedByCapturePrice = [...allData].sort((a,b) => b.capture_price - a.capture_price);
                const sortedByCaptureFloor0 = [...allData].sort((a,b) => b.capture_price_floor0 - a.capture_price_floor0);
                const sortedByCaptureRate = [...allData].sort((a,b) => b.capture_rate - a.capture_rate);
                const sortedBySolarNeg = [...allData].sort((a,b) => b.solar_at_neg_price_pct - a.solar_at_neg_price_pct);
                
                labels = sortedByNegHours.map(x => x.country);
                datasets = [
                    sortedByNegHours.map(x => x.neg_hours),
                    sortedByAvgPrice.map(x => x.avg_market_price),
                    sortedByCapturePrice.map(x => x.capture_price),
                    sortedByCaptureFloor0.map(x => x.capture_price_floor0),
                    sortedByCaptureRate.map(x => x.capture_rate),
                    sortedBySolarNeg.map(x => x.solar_at_neg_price_pct)
                ];
            } else if (zone !== 'all' && month === 'all') {
                // Find zone in yearly data (generation-weighted stats)
                const yd = yearlyData.find(x => x.country === zone);
                if (yd) {
                    d = { neg_hours: yd.neg_hours, avg_market_price: yd.avg_market_price,
                          capture_price: yd.capture_price, capture_price_floor0: yd.capture_price_floor0,
                          capture_rate: yd.capture_rate, solar_at_neg_price_pct: yd.solar_at_neg_price_pct };
                } else {
                    // Fallback to calculating from monthly data
                    const md_stats = monthlyData.filter(x => x.country === zone && x.year === parseInt(year));
                    d = {
                        neg_hours: md_stats.reduce((s,x) => s + x.neg_hours, 0),
                        avg_market_price: md_stats.length ? md_stats.reduce((s,x) => s + x.avg_market_price, 0) / md_stats.length : 0,
                        capture_price: md_stats.length ? md_stats.reduce((s,x) => s + x.capture_price, 0) / md_stats.length : 0,
                        capture_price_floor0: md_stats.length ? md_stats.reduce((s,x) => s + x.capture_price_floor0, 0) / md_stats.length : 0,
                        capture_rate: md_stats.length ? md_stats.reduce((s,x) => s + x.capture_rate, 0) / md_stats.length : 0,
                        solar_at_neg_price_pct: md_stats.length ? md_stats.reduce((s,x) => s + x.solar_at_neg_price_pct, 0) / md_stats.length : 0
                    };
                    console.warn('Zone not found in yearlyData:', zone, 'Available:', yearlyData.map(x => x.country));
                }
                const md = monthlyData.filter(x => x.country === zone && x.year === parseInt(year)).sort((a,b) => a.month - b.month);
                labels = md.map(x => MONTHS[x.month - 1]);
                datasets = [
                    md.map(x => x.neg_hours), md.map(x => x.avg_market_price),
                    md.map(x => x.capture_price), md.map(x => x.capture_price_floor0),
                    md.map(x => x.capture_rate), md.map(x => x.solar_at_neg_price_pct)
                ];
            } else if (zone === 'all' && month !== 'all') {
                const md = monthlyData.filter(x => x.month === parseInt(month) && x.year === parseInt(year));
                d = {
                    neg_hours: md.reduce((s,x) => s + x.neg_hours, 0),
                    avg_market_price: md.length ? md.reduce((s,x) => s + x.avg_market_price, 0) / md.length : 0,
                    capture_price: md.length ? md.reduce((s,x) => s + x.capture_price, 0) / md.length : 0,
                    capture_price_floor0: md.length ? md.reduce((s,x) => s + x.capture_price_floor0, 0) / md.length : 0,
                    capture_rate: md.length ? md.reduce((s,x) => s + x.capture_rate, 0) / md.length : 0,
                    solar_at_neg_price_pct: md.length ? md.reduce((s,x) => s + x.solar_at_neg_price_pct, 0) / md.length : 0
                };
                // Sort each dataset by its own metric (descending)
                const sortedByNegHours = [...md].sort((a,b) => b.neg_hours - a.neg_hours);
                const sortedByAvgPrice = [...md].sort((a,b) => b.avg_market_price - a.avg_market_price);
                const sortedByCapturePrice = [...md].sort((a,b) => b.capture_price - a.capture_price);
                const sortedByCaptureFloor0 = [...md].sort((a,b) => b.capture_price_floor0 - a.capture_price_floor0);
                const sortedByCaptureRate = [...md].sort((a,b) => b.capture_rate - a.capture_rate);
                const sortedBySolarNeg = [...md].sort((a,b) => b.solar_at_neg_price_pct - a.solar_at_neg_price_pct);
                
                labels = sortedByNegHours.map(x => x.country);
                datasets = [
                    sortedByNegHours.map(x => x.neg_hours),
                    sortedByAvgPrice.map(x => x.avg_market_price),
                    sortedByCapturePrice.map(x => x.capture_price),
                    sortedByCaptureFloor0.map(x => x.capture_price_floor0),
                    sortedByCaptureRate.map(x => x.capture_rate),
                    sortedBySolarNeg.map(x => x.solar_at_neg_price_pct)
                ];
            } else {
                const dailyRes = await fetch(`/api/summary/daily?country=${encodeURIComponent(zone)}&month=${month}&year=${year}`);
                const dailyData = (await dailyRes.json()).data;
                
                d = {
                    neg_hours: dailyData.reduce((s,x) => s + x.neg_hours, 0),
                    avg_market_price: dailyData.length ? dailyData.reduce((s,x) => s + x.avg_market_price, 0) / dailyData.length : 0,
                    capture_price: dailyData.length ? dailyData.reduce((s,x) => s + x.capture_price, 0) / dailyData.length : 0,
                    capture_price_floor0: dailyData.length ? dailyData.reduce((s,x) => s + x.capture_price_floor0, 0) / dailyData.length : 0,
                    capture_rate: dailyData.length ? dailyData.reduce((s,x) => s + x.capture_rate, 0) / dailyData.length : 0,
                    solar_at_neg_price_pct: dailyData.length ? dailyData.reduce((s,x) => s + x.solar_at_neg_price_pct, 0) / dailyData.length : 0
                };
                
                const sorted = dailyData.sort((a,b) => a.day - b.day);
                labels = sorted.map(x => x.day.toString());
                datasets = [
                    sorted.map(x => x.neg_hours), sorted.map(x => x.avg_market_price),
                    sorted.map(x => x.capture_price), sorted.map(x => x.capture_price_floor0),
                    sorted.map(x => x.capture_rate), sorted.map(x => x.solar_at_neg_price_pct)
                ];
            }
            
            document.getElementById('val1').textContent = (d.neg_hours||0).toFixed(1) + ' hrs';
            document.getElementById('val2').textContent = '€' + (d.avg_market_price||0).toFixed(2);
            document.getElementById('val3').textContent = '€' + (d.capture_price||0).toFixed(2);
            document.getElementById('val4').textContent = '€' + (d.capture_price_floor0||0).toFixed(2);
            document.getElementById('val5').textContent = (d.capture_rate||0).toFixed(1) + '%';
            document.getElementById('val6').textContent = (d.solar_at_neg_price_pct||0).toFixed(1) + '%';
            
            updateCharts(labels, datasets, zone, month, year);
        }
        
        // Helper function to set canvas size for scrolling
        function setCanvasSizeForScrolling(canvas, minWidth) {
            const container = canvas.parentElement;
            // Wait for layout to complete, then measure container
            requestAnimationFrame(() => {
                const containerWidth = container.offsetWidth || container.clientWidth || 400;
                // Canvas should be at least minWidth, ensuring it's wider than container if needed
                const canvasWidth = Math.max(minWidth, containerWidth + 1);
                
                // Set canvas dimensions
                canvas.width = canvasWidth;
                canvas.height = 250;
                canvas.style.width = canvasWidth + 'px';
                canvas.style.height = '250px';
                canvas.style.display = 'block';
            });
        }
        
        function updateCharts(labels, datasets, zone, month, year) {
            // Calculate minimum width based on number of bars (60px per bar minimum for readability)
            const minWidth = Math.max(labels.length * 60, 1000);
            
            // Store current filter values for click handler
            const currentZone = zone;
            const currentMonth = month;
            const currentYear = year;
            
            // Chart options
            const opts = {
                responsive: false,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false }
                },
                scales: {
                    x: {
                        ticks: { color: '#8892a0', maxRotation: 45, minRotation: 45 },
                        grid: { color: '#2a3a4d' }
                    },
                    y: {
                        ticks: { color: '#8892a0' },
                        grid: { color: '#2a3a4d' }
                    }
                }
            };
            
            for (let i = 0; i < 6; i++) {
                if (charts[i]) charts[i].destroy();
                
                const canvas = document.getElementById('chart' + (i+1));
                const container = canvas.parentElement; // This is the .chart-container div
                const chartCard = container.parentElement;
                
                // CRITICAL: Measure and lock container width BEFORE setting canvas size
                // Force container to respect its parent's width
                container.style.width = '100%';
                container.style.maxWidth = '100%';
                container.style.overflowX = 'scroll';
                container.style.overflowY = 'hidden';
                container.style.boxSizing = 'border-box';
                
                // Force a reflow to ensure container width is calculated
                void container.offsetWidth;
                
                // Now measure the container width (should be constrained to parent)
                const containerWidth = container.clientWidth || container.offsetWidth || 400;
                
                // Canvas must be wider than container to trigger scrolling
                const canvasWidth = Math.max(minWidth, containerWidth + 50);
                
                // Set canvas dimensions
                canvas.width = canvasWidth;
                canvas.height = 250;
                canvas.style.width = canvasWidth + 'px';
                canvas.style.height = '250px';
                canvas.style.display = 'block';
                canvas.style.maxWidth = 'none';
                canvas.style.minWidth = canvasWidth + 'px';
                canvas.style.boxSizing = 'content-box';
                
                const chartConfig = {
                    type: 'bar',
                    data: { labels, datasets: [{ data: datasets[i], backgroundColor: COLORS[i] + '99', borderColor: COLORS[i], borderWidth: 1 }] },
                    options: opts
                };
                
                charts[i] = new Chart(canvas, chartConfig);
                
                // After chart creation, ensure container hasn't expanded and canvas is correct size
                setTimeout(() => {
                    // Re-measure to check if container expanded
                    const newContainerWidth = container.clientWidth || container.offsetWidth;
                    const newCardWidth = chartCard.offsetWidth || chartCard.clientWidth;
                    
                    // If container expanded beyond card, force it back
                    if (newContainerWidth > newCardWidth * 1.1) {
                        container.style.width = '100%';
                        container.style.maxWidth = '100%';
                    }
                    
                    // Ensure canvas is still wide enough
                    const currentCanvasWidth = canvas.offsetWidth || parseInt(canvas.style.width) || 0;
                    if (currentCanvasWidth < minWidth) {
                        const finalCanvasWidth = Math.max(minWidth, newContainerWidth + 50);
                        canvas.width = finalCanvasWidth;
                        canvas.style.width = finalCanvasWidth + 'px';
                        canvas.style.minWidth = finalCanvasWidth + 'px';
                        if (charts[i]) {
                            charts[i].resize();
                        }
                    }
                }, 250);
                
                // Add click handler to chart6 (Solar Volume @ Neg Price) when viewing daily data
                if (i === 5 && currentZone !== 'all' && currentMonth !== 'all') {
                    canvas.style.cursor = 'pointer';
                    // Store values for the click handler
                    const clickZone = currentZone;
                    const clickMonth = parseInt(currentMonth);
                    const clickYear = parseInt(currentYear);
                    const clickLabels = [...labels];
                    const chartRef = charts[i];
                    
                    // Add click event listener to the canvas
                    canvas.addEventListener('click', function(evt) {
                        const points = chartRef.getElementsAtEventForMode(evt, 'nearest', { intersect: true }, true);
                        if (points.length) {
                            const firstPoint = points[0];
                            const dayIndex = firstPoint.index;
                            const day = parseInt(clickLabels[dayIndex]);
                            if (!isNaN(day) && day > 0 && day <= 31) {
                                openDayModal(clickZone, clickMonth, day, clickYear);
                            }
                        }
                    });
                } else if (i === 5) {
                    canvas.style.cursor = 'default';
                }
            }
        }
        
        let dayChart = null;
        
        function openDayModal(country, month, day, year) {
            const modal = document.getElementById('dayModal');
            const modalTitle = document.getElementById('modalTitle');
            modalTitle.textContent = `${country} - ${MONTHS[month-1]} ${day}, ${year}`;
            modal.style.display = 'block';
            
            // Load hourly data
            fetch(`/api/hourly/day?country=${encodeURIComponent(country)}&month=${month}&day=${day}&year=${year}`)
                .then(r => r.json())
                .then(data => {
                    displayDayChart(data);
                })
                .catch(err => {
                    console.error('Error loading day data:', err);
                    alert('Error loading hourly data for this day');
                });
        }
        
        function closeDayModal() {
            const modal = document.getElementById('dayModal');
            modal.style.display = 'none';
            if (dayChart) {
                dayChart.destroy();
                dayChart = null;
            }
        }
        
        function displayDayChart(data) {
            const canvas = document.getElementById('dayChart');
            if (dayChart) {
                dayChart.destroy();
            }
            
            // Format timestamps for display (HH:MM)
            const labels = data.data.map(d => {
                const dt = new Date(d.timestamp);
                return dt.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });
            });
            
            const prices = data.data.map(d => d.price);
            const solar = data.data.map(d => d.solar_mw);
            
            // Create separate datasets for positive and negative prices to highlight negative periods
            const positivePrices = prices.map(p => p >= 0 ? p : null);
            const negativePrices = prices.map(p => p < 0 ? p : null);
            
            dayChart = new Chart(canvas, {
                type: 'line',
                data: {
                    labels: labels,
                    datasets: [
                        {
                            label: 'Price (€/MWh)',
                            data: prices,
                            borderColor: '#4cc9f0',
                            backgroundColor: 'rgba(76, 201, 240, 0.4)',
                            fill: true,
                            yAxisID: 'y',
                            tension: 0.4,
                            pointRadius: 2,
                            pointHoverRadius: 4
                        },
                        {
                            label: 'Negative Price Periods',
                            data: negativePrices,
                            borderColor: '#f72585',
                            backgroundColor: 'rgba(247, 37, 133, 0.5)',
                            fill: true,
                            yAxisID: 'y',
                            tension: 0.4,
                            pointRadius: 0,
                            borderDash: [5, 5]
                        },
                        {
                            label: 'Solar Generation (MW)',
                            data: solar,
                            borderColor: '#fee440',
                            backgroundColor: 'rgba(254, 228, 64, 0.5)',
                            fill: true,
                            yAxisID: 'y1',
                            tension: 0.4,
                            pointRadius: 2,
                            pointHoverRadius: 4
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: {
                        mode: 'index',
                        intersect: false,
                    },
                    plugins: {
                        legend: {
                            display: true,
                            labels: {
                                color: '#8892a0'
                            }
                        },
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    if (context.datasetIndex === 0) {
                                        return `Price: €${context.parsed.y.toFixed(2)}/MWh`;
                                    } else if (context.datasetIndex === 1) {
                                        return `Negative Price: €${context.parsed.y.toFixed(2)}/MWh`;
                                    } else {
                                        return `Solar: ${context.parsed.y.toFixed(1)} MW`;
                                    }
                                }
                            }
                        }
                    },
                    scales: {
                        x: {
                            ticks: { color: '#8892a0' },
                            grid: { color: '#2a3a4d' }
                        },
                        y: {
                            type: 'linear',
                            display: true,
                            position: 'left',
                            ticks: { 
                                color: '#8892a0',
                                callback: function(value) {
                                    return '€' + value.toFixed(0);
                                }
                            },
                            grid: { color: '#2a3a4d' },
                            title: {
                                display: true,
                                text: 'Price (€/MWh)',
                                color: '#4cc9f0'
                            }
                        },
                        y1: {
                            type: 'linear',
                            display: true,
                            position: 'right',
                            ticks: { 
                                color: '#8892a0',
                                callback: function(value) {
                                    return value.toFixed(0) + ' MW';
                                }
                            },
                            grid: {
                                drawOnChartArea: false,
                            },
                            title: {
                                display: true,
                                text: 'Solar Generation (MW)',
                                color: '#fee440'
                            }
                        }
                    }
                }
            });
        }
        
        // Close modal when clicking outside
        window.onclick = function(event) {
            const modal = document.getElementById('dayModal');
            if (event.target === modal) {
                closeDayModal();
            }
        }
        
        // Window resize handler to recalculate chart canvas sizes
        let resizeTimeout;
        window.addEventListener('resize', function() {
            clearTimeout(resizeTimeout);
            resizeTimeout = setTimeout(function() {
                // Recalculate canvas sizes for all charts
                for (let i = 0; i < 6; i++) {
                    if (charts[i]) {
                        const canvas = document.getElementById('chart' + (i+1));
                        const container = canvas.parentElement;
                        const containerWidth = container.offsetWidth || container.clientWidth || 400;
                        const labels = charts[i].data.labels || [];
                        const minWidth = Math.max(labels.length * 60, 1000);
                        const canvasWidth = Math.max(minWidth, containerWidth + 1);
                        
                        if (canvas.offsetWidth !== canvasWidth) {
                            canvas.width = canvasWidth;
                            canvas.style.width = canvasWidth + 'px';
                            charts[i].resize();
                        }
                    }
                }
            }, 250);
        });
        
        document.getElementById('zoneSelect').addEventListener('change', updateDisplay);
        document.getElementById('monthSelect').addEventListener('change', updateDisplay);
        document.getElementById('yearSelect').addEventListener('change', updateDisplay);
        loadData();
    </script>
</body>
</html>
""")


@app.get("/about/project")
def about_project():
    styles = get_base_styles()
    nav = get_nav_html('project')
    return HTMLResponse(f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>About the Project - enerlyzer</title>
    <style>{styles}</style>
</head>
<body>
    {nav}
    <div class="page-content">
        <header>
            <h1>enerlyzer</h1>
            <p class="subtitle">About the Project</p>
        </header>
        
        <h2>What is enerlyzer?</h2>
        <p>
            enerlyzer is a comprehensive dashboard for analyzing the European energy market, 
            with a focus on solar power economics. The platform tracks and visualizes key metrics 
            across 47 European bidding zones, providing insights into market dynamics, 
            negative pricing events, and renewable energy capture rates.
        </p>
        
        <h2>Why This Matters</h2>
        <p>
            As Europe accelerates its transition to renewable energy, the dynamics of electricity 
            markets are changing rapidly — especially with the rise of solar. New patterns are 
            emerging and are reshaping how energy systems operate. Negative price hours, for example, 
            highlight both the challenges and opportunities that come with a power system increasingly 
            driven by weather-dependent generation.
        </p>
        <p>
            This project aims to shed light on these evolving dynamics and make them easier to understand. 
            Building the app also gave me the opportunity to explore energy datasets more deeply using 
            SQL and Python and to experiment with ways of visualizing market behavior.
        </p>
        <p>
            The work is ongoing, and I plan to expand the tool step by step with new features, 
            more analytics, and additional market indicators.
        </p>
        <p>
            This dashboard helps investors, researchers, and policymakers understand:
        </p>
        <ul style="color: var(--text-muted); margin-left: 2rem; line-height: 2;">
            <li>Where and when negative pricing occurs</li>
            <li>How solar generators are affected by market dynamics</li>
            <li>The gap between average market prices and solar capture prices</li>
            <li>Trends across different European markets</li>
        </ul>
        
        <h2>Data Source</h2>
        <p>
            All data is sourced from the <strong>ENTSO-E Transparency Platform</strong>, 
            the official source for European electricity market data. The dashboard updates 
            daily with the latest market information.
        </p>
    </div>
</body>
</html>
""")


@app.get("/about/me")
def about_me():
    styles = get_base_styles()
    nav = get_nav_html('about-me')
    return HTMLResponse(f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>About Me - enerlyzer</title>
    <style>{styles}</style>
</head>
<body>
    {nav}
    <div class="page-content">
        <header>
            <h1>enerlyzer</h1>
            <p class="subtitle">About Me</p>
        </header>
        
        <div class="profile-section">
            <img src="/static/250509_PGB9975_1.jpg" alt="Profile" class="profile-img" 
                 onerror="this.src='https://via.placeholder.com/180?text=Photo'">
            <div>
                <h2 style="margin-top: 0;">Marian Willuhn</h2>
                <p>
                    Hi, I'm Marian Willuhn, an energy nerd with a background in political science 
                    and international law, currently working as a journalist at the intersection of 
                    electricity markets, regulation, and data. Over the past years, I've been exploring 
                    how energy systems evolve: how prices form, where flexibility is needed, and which 
                    role storage, grids, and digitalization play in the transition towards a renewable future.
                </p>
                <p>
                    I have been exploring data-driven market and price analysis and have been building 
                    tools that make public energy datasets more accessible. In doing so, I worked with 
                    Python and SQL, and I developed APIs and data infrastructure to visualize trends 
                    and uncover insights. This app is part of that journey — a way to turn raw electricity 
                    market data into something transparent, interactive, and useful.
                </p>
                <p>
                    My motivation is to turn complex energy topics into something everybody understands. 
                    I enjoy breaking down technical or regulatory issues and turning them into clear 
                    insights that help people grasp what is happening in the power system and why it matters.
                </p>
                <p>
                    If you have ideas, want to collaborate, or need help with energy data analysis or 
                    market research, feel free to reach out.
                </p>
            </div>
        </div>
        
        <div class="social-links">
            <a href="https://www.linkedin.com/in/marian-willuhn-0451b2a8/" target="_blank">
                <svg viewBox="0 0 24 24" fill="currentColor">
                    <path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433c-1.144 0-2.063-.926-2.063-2.065 0-1.138.92-2.063 2.063-2.063 1.14 0 2.064.925 2.064 2.063 0 1.139-.925 2.065-2.064 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"/>
                </svg>
                LinkedIn
            </a>
            <a href="https://github.com/Willyuhn" target="_blank">
                <svg viewBox="0 0 24 24" fill="currentColor">
                    <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/>
                </svg>
                GitHub
            </a>
            <a href="mailto:willuhn.marian@gmail.com">
                <svg viewBox="0 0 24 24" fill="currentColor">
                    <path d="M20 4H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zm0 4l-8 5-8-5V6l8 5 8-5v2z"/>
                </svg>
                Email
            </a>
        </div>
    </div>
</body>
</html>
""")


@app.get("/methodology")
def methodology():
    styles = get_base_styles()
    nav = get_nav_html('methodology')
    return HTMLResponse(f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Methodology - enerlyzer</title>
    <style>{styles}</style>
</head>
<body>
    {nav}
    <div class="page-content">
        <header>
            <h1>enerlyzer</h1>
            <p class="subtitle">Methodology</p>
        </header>
        
        <h2>How We Calculate the Metrics</h2>
        
        <div class="method-card">
            <h3>Negative Price Hours</h3>
            <p>Hours where the day-ahead electricity price falls below €0/MWh.</p>
            <div class="formula">Neg Hours = Σ (hours where Price &lt; 0)</div>
            <p>For 15-minute resolution data, each interval counts as 0.25 hours.</p>
        </div>
        
        <div class="method-card">
            <h3>Average Market Price</h3>
            <p>The arithmetic mean of all day-ahead hourly prices in the selected period.</p>
            <div class="formula">Avg Price = Σ(Price) / Count(Hours)</div>
        </div>
        
        <div class="method-card">
            <h3>Capture Price</h3>
            <p>The volume-weighted average price received by solar generators.</p>
            <div class="formula">Capture Price = Σ(Generation × Price) / Σ(Generation)</div>
        </div>
        
        <div class="method-card">
            <h3>Capture Price (Floor €0)</h3>
            <p>Same as Capture Price, but negative prices are floored at €0.</p>
            <div class="formula">Capture Floor = Σ(Generation × max(Price, 0)) / Σ(Generation)</div>
        </div>
        
        <div class="method-card">
            <h3>Capture Rate</h3>
            <p>The ratio of Capture Price to Average Market Price.</p>
            <div class="formula">Capture Rate = (Capture Price / Avg Market Price) × 100%</div>
        </div>
        
        <div class="method-card">
            <h3>Solar Volume at Negative Prices</h3>
            <p>Percentage of solar generation during negative price hours.</p>
            <div class="formula">Solar @ Neg = (Gen during neg hours / Total Gen) × 100%</div>
        </div>
    </div>
</body>
</html>
""")


@app.get("/contact")
def contact():
    styles = get_base_styles()
    nav = get_nav_html('contact')
    return HTMLResponse(f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Contact - enerlyzer</title>
    <style>{styles}</style>
</head>
<body>
    {nav}
    <div class="page-content">
        <header>
            <h1>enerlyzer</h1>
            <p class="subtitle">Contact</p>
        </header>
        
        <h2>Get in Touch</h2>
        <p>
            Have questions about the data, methodology, or interested in collaboration? 
            Feel free to reach out through any of the channels below.
        </p>
        
        <div class="social-links" style="justify-content: center; margin-top: 3rem;">
            <a href="https://www.linkedin.com/in/marian-willuhn-0451b2a8/" target="_blank">
                <svg viewBox="0 0 24 24" fill="currentColor">
                    <path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433c-1.144 0-2.063-.926-2.063-2.065 0-1.138.92-2.063 2.063-2.063 1.14 0 2.064.925 2.064 2.063 0 1.139-.925 2.065-2.064 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"/>
                </svg>
                LinkedIn
            </a>
            <a href="https://github.com/Willyuhn" target="_blank">
                <svg viewBox="0 0 24 24" fill="currentColor">
                    <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/>
                </svg>
                GitHub
            </a>
            <a href="mailto:willuhn.marian@gmail.com">
                <svg viewBox="0 0 24 24" fill="currentColor">
                    <path d="M20 4H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zm0 4l-8 5-8-5V6l8 5 8-5v2z"/>
                </svg>
                Email
            </a>
        </div>
        
        <div style="text-align: center; margin-top: 3rem; padding: 2rem; background: var(--bg-card); border-radius: 12px;">
            <p style="margin-bottom: 0;">
                <strong style="color: var(--cyan);">enerlyzer</strong> is an open-source project.<br>
                Contributions and feedback are welcome!
            </p>
        </div>
    </div>
</body>
</html>
""")


@app.get("/static/{filename}")
def serve_static(filename: str):
    import logging
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    static_path = os.path.join(static_dir, filename)
    
    # Log for debugging
    logging.info(f"Static file request: {filename}")
    logging.info(f"Static dir: {static_dir}, exists: {os.path.exists(static_dir)}")
    logging.info(f"Static path: {static_path}, exists: {os.path.exists(static_path)}")
    
    if os.path.exists(static_dir):
        files_in_dir = os.listdir(static_dir)
        logging.info(f"Files in static dir: {files_in_dir}")
    
    if os.path.exists(static_path):
        # Determine media type based on file extension
        media_type = None
        if filename.lower().endswith(('.jpg', '.jpeg')):
            media_type = 'image/jpeg'
        elif filename.lower().endswith('.png'):
            media_type = 'image/png'
        elif filename.lower().endswith('.gif'):
            media_type = 'image/gif'
        return FileResponse(static_path, media_type=media_type)
    
    # Return more detailed error
    return HTMLResponse(
        f"File not found: {filename}<br>Static dir: {static_dir}<br>Files: {', '.join(os.listdir(static_dir)) if os.path.exists(static_dir) else 'Directory does not exist'}",
        status_code=404
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
