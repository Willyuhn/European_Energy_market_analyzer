"""
European Energy Market Dashboard
Full featured with capture prices and summary tables
"""

import os
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import mysql.connector

app = FastAPI(title="European Energy Market Dashboard")

# Database configuration (set via environment variables)
DB_HOST = os.environ['DB_HOST']
DB_PORT = int(os.getenv('DB_PORT', '3306'))
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_NAME = os.getenv('DB_NAME', 'energy_market')


def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, use_pure=True, connection_timeout=30
    )


@app.get("/health")
def health_check():
    """Health check for Cloud Run"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.post("/admin/update-summaries")
def update_summaries(secret: str = ""):
    """
    Recalculate all summary tables.
    Triggered by Cloud Scheduler at 6 AM daily.
    Requires secret key for security.
    """
    # Simple security check
    expected_secret = os.getenv("UPDATE_SECRET", "your-secret-key")
    if secret != expected_secret:
        return {"error": "Unauthorized"}, 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Recalculate summary_monthly
        cursor.execute("DELETE FROM summary_monthly")
        cursor.execute("""
            INSERT INTO summary_monthly (country, month, neg_hours, avg_market_price)
            WITH prices_raw AS (
                SELECT AreaDisplayName, `DateTime(UTC)`, ResolutionCode, `Price[Currency/MWh]`, source_month,
                    SUM(CASE WHEN ResolutionCode = 'PT60M' THEN 1 ELSE 0 END) 
                    OVER (PARTITION BY AreaDisplayName, `DateTime(UTC)`) AS cnt_60m_same_ts
                FROM energy_prices
                WHERE ContractType = 'Day-ahead' AND (`Sequence` IS NULL OR `Sequence` NOT IN ('2', '3'))
            ),
            prices_dedup AS (
                SELECT * FROM prices_raw
                WHERE ResolutionCode = 'PT60M' OR (ResolutionCode = 'PT15M' AND cnt_60m_same_ts = 0)
            )
            SELECT AreaDisplayName, source_month,
                SUM(CASE WHEN ResolutionCode = 'PT60M' AND `Price[Currency/MWh]` < 0 THEN 1.0
                         WHEN ResolutionCode = 'PT15M' AND `Price[Currency/MWh]` < 0 THEN 0.25 ELSE 0.0 END),
                ROUND(AVG(`Price[Currency/MWh]`), 2)
            FROM prices_dedup GROUP BY AreaDisplayName, source_month
        """)
        conn.commit()
        
        # Recalculate summary_yearly
        cursor.execute("DELETE FROM summary_yearly")
        cursor.execute("""
            INSERT INTO summary_yearly (country, total_neg_hours, avg_market_price)
            SELECT country, SUM(neg_hours), ROUND(AVG(avg_market_price), 2)
            FROM summary_monthly GROUP BY country
        """)
        conn.commit()
        
        # Recalculate summary_total
        cursor.execute("DELETE FROM summary_total")
        cursor.execute("""
            INSERT INTO summary_total (id, total_neg_hours, avg_market_price)
            SELECT 1, SUM(total_neg_hours), ROUND(AVG(avg_market_price), 2)
            FROM summary_yearly
        """)
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return {"status": "success", "message": "Summary tables updated"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


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
    """Get daily summary for a specific country and month"""
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


@app.get("/")
def home():
    return HTMLResponse("""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>European Energy Market Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
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
            padding: 2rem;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        header { text-align: center; margin-bottom: 2rem; }
        h1 {
            font-size: 2rem;
            background: linear-gradient(135deg, var(--cyan), var(--pink));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 0.5rem;
        }
        .subtitle { color: var(--text-muted); }
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
    <div class="container">
        <header>
            <h1>⚡ European Energy Market Dashboard</h1>
            <p class="subtitle">Solar Capture Prices & Negative Price Hours • 2025</p>
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
                // Single zone + single month -> fetch DAILY data
                const dailyRes = await fetch(`/api/summary/daily?country=${encodeURIComponent(zone)}&month=${month}`);
                const dailyData = (await dailyRes.json()).data;
                
                // Calculate totals for stat cards
                d = {
                    neg_hours: dailyData.reduce((s,x) => s + x.neg_hours, 0),
                    avg_market_price: dailyData.length ? dailyData.reduce((s,x) => s + x.avg_market_price, 0) / dailyData.length : 0,
                    capture_price: dailyData.length ? dailyData.reduce((s,x) => s + x.capture_price, 0) / dailyData.length : 0,
                    capture_price_floor0: dailyData.length ? dailyData.reduce((s,x) => s + x.capture_price_floor0, 0) / dailyData.length : 0,
                    capture_rate: dailyData.length ? dailyData.reduce((s,x) => s + x.capture_rate, 0) / dailyData.length : 0,
                    solar_at_neg_price_pct: dailyData.length ? dailyData.reduce((s,x) => s + x.solar_at_neg_price_pct, 0) / dailyData.length : 0
                };
                
                // Daily labels (1, 2, 3, ... 31)
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
