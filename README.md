# ⚡ European Energy Market Dashboard

A web application for visualizing solar capture prices, negative price hours, and market price analytics across European electricity bidding zones.

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.109-green.svg)
![DuckDB](https://img.shields.io/badge/DuckDB-0.10-yellow.svg)

## Features

- **Negative Price Hours**: Track cumulative hours with negative electricity prices per bidding zone
- **Average Market Price**: Day-ahead wholesale electricity prices (€/MWh)
- **Solar Capture Price**: Revenue-weighted price received by solar generators
- **Capture Price (Floor 0)**: Capture price with negative prices floored to zero
- **Solar at Negative Prices**: Percentage of solar generation during negative price periods

### Interactive Controls

- **47 European Bidding Zones**: Including Germany-Luxembourg, Nordic zones, Italian zones, and more
- **Monthly & Daily Granularity**: View data by month or drill down to daily values
- **Dynamic Charts**: All metrics visualized as interactive bar charts

## Data Source

Data sourced from [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/):
- `EnergyPrices_12.1.D` - Day-ahead electricity prices
- `AggregatedGenerationPerType_16.1.B_C` - Generation by production type (Solar)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/YOUR_USERNAME/european-energy-dashboard.git
cd european-energy-dashboard
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Add your ENTSO-E data files:
   - Place CSV files in the project root directory
   - Naming format: `2025_XX_EnergyPrices_12.1.D_r3.csv` and `2025_XX_AggregatedGenerationPerType_16.1.B_C.csv`

4. Run the application:
```bash
python app.py
```

5. Open your browser at [http://localhost:8000](http://localhost:8000)

## Tech Stack

- **Backend**: FastAPI + Uvicorn
- **Database**: DuckDB (in-memory SQL analytics)
- **Frontend**: Vanilla JS + Chart.js
- **Styling**: Custom CSS with JetBrains Mono & Sora fonts

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Main dashboard HTML |
| `GET /api/data` | Monthly aggregated market data |
| `GET /api/daily-data` | Daily granular market data |
| `GET /api/countries` | List of available bidding zones |

## Metrics Explained

### Negative Price Hours
Sum of hours where the day-ahead electricity price was below €0/MWh. For 15-minute resolution markets, each negative interval counts as 0.25 hours.

### Capture Price
The generation-weighted average price received by solar generators:
```
Capture Price = Σ(Generation × Price) / Σ(Generation)
```

### Capture Price (Floor 0)
Same as capture price, but negative prices are treated as €0 - representing revenue with a price floor protection.

### Solar at Negative Prices (%)
Percentage of total solar generation that occurred during negative price hours.

## License

MIT License

## Author

Built with ☀️ for European energy market analysis

