"""
European Energy Market Dashboard
Full featured with capture prices and summary tables
"""

import os
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, FileResponse
import mysql.connector

app = FastAPI(title="European Energy Market Dashboard")

# Database configuration (set via environment variables in Cloud Run)
# These must be set as environment variables - no defaults for security
DB_HOST = os.environ['DB_HOST']
DB_PORT = int(os.environ.get('DB_PORT', '3306'))
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_NAME = os.environ.get('DB_NAME', 'energy_market')


def get_db_connection():
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


@app.get("/api/summary/monthly")
def get_summary_monthly():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT country, month, neg_hours, avg_market_price, capture_price,
               capture_price_floor0, capture_rate, solar_at_neg_price_pct
        FROM summary_monthly ORDER BY country, month
    """)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    data = [{
        "country": r[0], "month": r[1], "neg_hours": float(r[2] or 0), 
        "avg_market_price": float(r[3] or 0), "capture_price": float(r[4] or 0),
        "capture_price_floor0": float(r[5] or 0), "capture_rate": float(r[6] or 0),
        "solar_at_neg_price_pct": float(r[7] or 0)
    } for r in results]
    return {"data": data}


@app.get("/api/summary/daily")
def get_summary_daily(country: str = None, month: int = None):
    conn = get_db_connection()
    cursor = conn.cursor()
    
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
            FROM summary_daily ORDER BY country, month, day
        """)
    
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    data = [{
        "country": r[0], "month": r[1], "day": r[2], "neg_hours": float(r[3] or 0), 
        "avg_market_price": float(r[4] or 0), "capture_price": float(r[5] or 0),
        "capture_price_floor0": float(r[6] or 0), "capture_rate": float(r[7] or 0),
        "solar_at_neg_price_pct": float(r[8] or 0)
    } for r in results]
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
        .container { max-width: 1400px; margin: 0 auto; padding: 2rem; }
        
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
            gap: 1.25rem;
        }
        @media (max-width: 1200px) { .charts { grid-template-columns: repeat(2, 1fr); } }
        @media (max-width: 768px) { .charts { grid-template-columns: 1fr; } }
        .chart-card {
            background: var(--bg-card);
            border-radius: 12px;
            padding: 1.25rem;
        }
        .chart-title { font-size: 0.9rem; margin-bottom: 0.75rem; color: var(--text-muted); }
        .chart-container { height: 250px; }
    </style>
</head>
<body>
    """ + nav + """
    <div class="container">
        <header>
            <h1>enerlyzer</h1>
            <p class="subtitle">European Energy Market Dashboard • Solar Capture Prices & Negative Hours</p>
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
            
    <script>
        let yearlyData = [], monthlyData = [], totalData = {};
        let charts = {};
        const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        const COLORS = ['#00f5d4','#f72585','#fee440','#ff6b35','#9d4edd','#4cc9f0'];
        
        async function loadData() {
            const [t, y, m] = await Promise.all([
                fetch('/api/summary/total').then(r => r.json()),
                fetch('/api/summary/yearly').then(r => r.json()),
                fetch('/api/summary/monthly').then(r => r.json())
            ]);
            totalData = t; yearlyData = y.data; monthlyData = m.data;
            updateDisplay();
        }
        
        async function updateDisplay() {
            const zone = document.getElementById('zoneSelect').value;
            const month = document.getElementById('monthSelect').value;
            let d, labels, datasets;
            
            if (zone === 'all' && month === 'all') {
                d = totalData;
                const sorted = [...yearlyData].sort((a,b) => b.neg_hours - a.neg_hours).slice(0, 12);
                labels = sorted.map(x => x.country.substring(0, 12));
                datasets = [
                    sorted.map(x => x.neg_hours), sorted.map(x => x.avg_market_price),
                    sorted.map(x => x.capture_price), sorted.map(x => x.capture_price_floor0),
                    sorted.map(x => x.capture_rate), sorted.map(x => x.solar_at_neg_price_pct)
                ];
            } else if (zone !== 'all' && month === 'all') {
                const yd = yearlyData.find(x => x.country === zone) || {};
                d = { neg_hours: yd.neg_hours||0, avg_market_price: yd.avg_market_price||0,
                      capture_price: yd.capture_price||0, capture_price_floor0: yd.capture_price_floor0||0,
                      capture_rate: yd.capture_rate||0, solar_at_neg_price_pct: yd.solar_at_neg_price_pct||0 };
                const md = monthlyData.filter(x => x.country === zone).sort((a,b) => a.month - b.month);
                labels = md.map(x => MONTHS[x.month - 1]);
                datasets = [
                    md.map(x => x.neg_hours), md.map(x => x.avg_market_price),
                    md.map(x => x.capture_price), md.map(x => x.capture_price_floor0),
                    md.map(x => x.capture_rate), md.map(x => x.solar_at_neg_price_pct)
                ];
            } else if (zone === 'all' && month !== 'all') {
                const md = monthlyData.filter(x => x.month === parseInt(month));
                d = {
                    neg_hours: md.reduce((s,x) => s + x.neg_hours, 0),
                    avg_market_price: md.length ? md.reduce((s,x) => s + x.avg_market_price, 0) / md.length : 0,
                    capture_price: md.length ? md.reduce((s,x) => s + x.capture_price, 0) / md.length : 0,
                    capture_price_floor0: md.length ? md.reduce((s,x) => s + x.capture_price_floor0, 0) / md.length : 0,
                    capture_rate: md.length ? md.reduce((s,x) => s + x.capture_rate, 0) / md.length : 0,
                    solar_at_neg_price_pct: md.length ? md.reduce((s,x) => s + x.solar_at_neg_price_pct, 0) / md.length : 0
                };
                const sorted = [...md].sort((a,b) => b.neg_hours - a.neg_hours).slice(0, 12);
                labels = sorted.map(x => x.country.substring(0, 12));
                datasets = [
                    sorted.map(x => x.neg_hours), sorted.map(x => x.avg_market_price),
                    sorted.map(x => x.capture_price), sorted.map(x => x.capture_price_floor0),
                    sorted.map(x => x.capture_rate), sorted.map(x => x.solar_at_neg_price_pct)
                ];
            } else {
                const dailyRes = await fetch(`/api/summary/daily?country=${encodeURIComponent(zone)}&month=${month}`);
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
            
            updateCharts(labels, datasets);
        }
        
        function updateCharts(labels, datasets) {
            const opts = {
                responsive: true, maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                    scales: {
                    x: { ticks: { color: '#8892a0', maxRotation: 45 }, grid: { color: '#2a3a4d' } },
                    y: { ticks: { color: '#8892a0' }, grid: { color: '#2a3a4d' } }
                }
            };
            for (let i = 0; i < 6; i++) {
                if (charts[i]) charts[i].destroy();
                charts[i] = new Chart(document.getElementById('chart' + (i+1)), {
                type: 'bar',
                    data: { labels, datasets: [{ data: datasets[i], backgroundColor: COLORS[i] + '99', borderColor: COLORS[i], borderWidth: 1 }] },
                    options: opts
                });
            }
        }
        
        document.getElementById('zoneSelect').addEventListener('change', updateDisplay);
        document.getElementById('monthSelect').addEventListener('change', updateDisplay);
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
    static_path = os.path.join(os.path.dirname(__file__), "static", filename)
    if os.path.exists(static_path):
        return FileResponse(static_path)
    return HTMLResponse("Not found", status_code=404)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
