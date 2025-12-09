# European Energy Market Dashboard

A real-time dashboard visualizing European electricity market data, focusing on solar capture prices and negative price hours across 47 bidding zones.

![Dashboard Preview](https://img.shields.io/badge/Status-Live-brightgreen)

## Features

- **6 Key Metrics** per bidding zone:
  - Negative Price Hours
  - Average Market Price (€/MWh)
  - Solar Capture Price (€/MWh)
  - Capture Price with Floor at €0
  - Capture Rate (%)
  - Solar Volume at Negative Prices (%)

- **Interactive Filtering**:
  - 47 European bidding zones
  - Monthly and yearly views
  - Daily granularity when selecting single zone + month

- **Instant Loading**: Pre-computed summary tables for fast performance

## Data Source

[ENTSO-E Transparency Platform](https://transparency.entsoe.eu/)
- Day-ahead energy prices (12.1.D)
- Aggregated generation per type (16.1.B&C)

## Tech Stack

- **Backend**: FastAPI + Python
- **Database**: Google Cloud SQL (MySQL)
- **Frontend**: Chart.js
- **Deployment**: Google Cloud Run

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables (required!)
export DB_HOST="your-cloud-sql-ip"
export DB_PORT="3306"
export DB_USER="your-username"
export DB_PASSWORD="your-password"
export DB_NAME="energy_market"

# Or copy env.example and source it
cp env.example .env
# Edit .env with your values
source .env

# Run the app
python app.py
```

Open http://localhost:8080

## Project Structure

```
├── app.py                      # Main FastAPI application
├── requirements.txt            # Python dependencies
├── Dockerfile                  # Cloud Run deployment
├── scripts/
│   ├── create_summary_tables.py    # Create pre-computed summary tables
│   ├── calculate_daily_capture.py  # Calculate daily capture metrics
│   └── upload_generation_new.py    # Upload generation data to Cloud SQL
└── README.md
```

## Database Schema

### Summary Tables (pre-computed for fast queries)

- `summary_total` - Overall totals across all zones/year
- `summary_yearly` - Aggregated per country, full year
- `summary_monthly` - Per country, per month
- `summary_daily` - Per country, per month, per day

### Raw Data Tables

- `energy_prices` - Day-ahead electricity prices
- `generation_per_type` - Solar/Wind generation data

## Deployment to Cloud Run

1. Build and push Docker image:
```bash
gcloud builds submit --tag gcr.io/PROJECT_ID/energy-dashboard
```

2. Deploy to Cloud Run:
```bash
gcloud run deploy energy-dashboard \
  --image gcr.io/PROJECT_ID/energy-dashboard \
  --platform managed \
  --region europe-west1 \
  --set-env-vars DB_HOST=xxx,DB_USER=xxx,DB_PASSWORD=xxx,DB_NAME=energy_market
```

## License

MIT
