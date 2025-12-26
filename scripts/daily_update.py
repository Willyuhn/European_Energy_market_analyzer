"""
ENTSO-E Daily Update Script
Fetches new Energy Prices and Generation data from ENTSO-E API
and updates the Google Cloud SQL database.
Designed to run as a Cloud Run Job triggered by Cloud Scheduler.
"""

import os
import sys
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import mysql.connector
import time

# =============================================================================
# CONFIGURATION - All from environment variables
# =============================================================================

ENTSOE_API_TOKEN = os.environ.get('ENTSOE_API_TOKEN', '')
ENTSOE_BASE_URL = "https://web-api.tp.entsoe.eu/api"

DB_CONFIG = {
    'host': os.environ['DB_HOST'],
    'port': int(os.environ.get('DB_PORT', '3306')),
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'database': os.environ.get('DB_NAME', 'energy_market'),
    'use_pure': True,
    'connection_timeout': 300,
}

PRODUCTION_TYPES = {
    'B16': 'Solar',
    'B18': 'Wind Offshore', 
    'B19': 'Wind Onshore',
}

BIDDING_ZONES = {
    '10YAT-APG------L': 'Austria (AT)',
    '10YBE----------2': 'Belgium (BE)',
    '10YCZ-CEPS-----N': 'Czech Republic (CZ)',
    '10Y1001A1001A82H': 'DE-LU',
    '10YDK-1--------W': 'DK1',
    '10YDK-2--------M': 'DK2',
    '10YFI-1--------U': 'Finland (FI)',
    '10YFR-RTE------C': 'France (FR)',
    '10YGR-HTSO-----Y': 'Greece (GR)',
    '10YHU-MAVIR----U': 'Hungary (HU)',
    '10Y1001A1001A59C': 'IE(SEM)',
    '10Y1001A1001A73I': 'IT-North',
    '10Y1001A1001A70O': 'IT-Centre-North',
    '10Y1001A1001A71M': 'IT-Centre-South',
    '10Y1001A1001A72K': 'IT-South',
    '10Y1001A1001A74G': 'IT-Sicily',
    '10Y1001A1001A75E': 'IT-Sardinia',
    '10YNL----------L': 'Netherlands (NL)',
    '10YNO-1--------2': 'NO1',
    '10YNO-2--------T': 'NO2',
    '10YNO-3--------J': 'NO3',
    '10YNO-4--------9': 'NO4',
    '10Y1001A1001A48H': 'NO5',
    '10YPL-AREA-----S': 'Poland (PL)',
    '10YPT-REN------W': 'Portugal (PT)',
    '10YES-REE------0': 'Spain (ES)',
    '10Y1001A1001A44P': 'SE1',
    '10Y1001A1001A45N': 'SE2',
    '10Y1001A1001A46L': 'SE3',
    '10Y1001A1001A47J': 'SE4',
    '10YSK-SEPS-----K': 'Slovakia (SK)',
    '10YSI-ELES-----O': 'Slovenia (SI)',
    '10YCH-SWISSGRIDZ': 'Switzerland (CH)',
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

def parse_datetime(dt_str):
    try:
        return datetime.strptime(dt_str.replace('Z', ''), '%Y-%m-%dT%H:%M')
    except:
        return None

def format_period(dt):
    return dt.strftime('%Y%m%d%H%M')

# =============================================================================
# ENTSO-E API FUNCTIONS
# =============================================================================

def fetch_day_ahead_prices(area_code, start_date, end_date):
    params = {
        'securityToken': ENTSOE_API_TOKEN,
        'documentType': 'A44',
        'in_Domain': area_code,
        'out_Domain': area_code,
        'periodStart': format_period(start_date),
        'periodEnd': format_period(end_date),
    }
    
    try:
        response = requests.get(ENTSOE_BASE_URL, params=params, timeout=60)
        if response.status_code == 200:
            return parse_price_xml(response.text, area_code)
        return []
    except Exception as e:
        print(f"   ❌ API error: {e}")
        return []

def parse_price_xml(xml_text, area_code):
    prices = []
    try:
        root = ET.fromstring(xml_text)
        ns = {'ns': 'urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3'}
        area_name = BIDDING_ZONES.get(area_code, area_code)
        
        for timeseries in root.findall('.//ns:TimeSeries', ns):
            currency = timeseries.find('.//ns:currency_Unit.name', ns)
            for period in timeseries.findall('.//ns:Period', ns):
                start_elem = period.find('.//ns:timeInterval/ns:start', ns)
                resolution = period.find('.//ns:resolution', ns)
                if start_elem is None:
                    continue
                period_start = parse_datetime(start_elem.text)
                res_code = 'PT60M'
                res_minutes = 60
                if resolution is not None and 'PT15M' in resolution.text:
                    res_minutes = 15
                    res_code = 'PT15M'
                
                for point in period.findall('.//ns:Point', ns):
                    position = int(point.find('ns:position', ns).text)
                    price_elem = point.find('ns:price.amount', ns)
                    if price_elem is not None:
                        price = float(price_elem.text)
                        timestamp = period_start + timedelta(minutes=(position - 1) * res_minutes)
                        prices.append({
                            'datetime_utc': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                            'area_code': area_code,
                            'area_name': area_name,
                            'price': price,
                            'resolution': res_code,
                            'currency': currency.text if currency is not None else 'EUR',
                            'month': timestamp.month,
                        })
    except ET.ParseError as e:
        print(f"   ⚠️ XML parse error: {e}")
    return prices

def fetch_generation(area_code, psr_type, start_date, end_date):
    params = {
        'securityToken': ENTSOE_API_TOKEN,
        'documentType': 'A75',
        'processType': 'A16',
        'in_Domain': area_code,
        'psrType': psr_type,
        'periodStart': format_period(start_date),
        'periodEnd': format_period(end_date),
    }
    
    try:
        response = requests.get(ENTSOE_BASE_URL, params=params, timeout=60)
        if response.status_code == 200:
            return parse_generation_xml(response.text, area_code, psr_type)
        return []
    except Exception as e:
        print(f"   ❌ API error: {e}")
        return []

def parse_generation_xml(xml_text, area_code, psr_type):
    generation = []
    try:
        root = ET.fromstring(xml_text)
        ns = {'ns': 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'}
        area_name = BIDDING_ZONES.get(area_code, area_code)
        
        for timeseries in root.findall('.//ns:TimeSeries', ns):
            for period in timeseries.findall('.//ns:Period', ns):
                start_elem = period.find('.//ns:timeInterval/ns:start', ns)
                resolution = period.find('.//ns:resolution', ns)
                if start_elem is None:
                    continue
                period_start = parse_datetime(start_elem.text)
                res_code = 'PT60M'
                res_minutes = 60
                if resolution is not None and 'PT15M' in resolution.text:
                    res_minutes = 15
                    res_code = 'PT15M'
                
                for point in period.findall('.//ns:Point', ns):
                    position = int(point.find('ns:position', ns).text)
                    quantity = point.find('ns:quantity', ns)
                    if quantity is not None:
                        output = float(quantity.text)
                        timestamp = period_start + timedelta(minutes=(position - 1) * res_minutes)
                        generation.append({
                            'datetime': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                            'area_code': area_code,
                            'area_name': area_name,
                            'production_type': PRODUCTION_TYPES.get(psr_type, psr_type),
                            'output': output,
                            'resolution': res_code,
                            'month': timestamp.month,
                        })
    except ET.ParseError as e:
        print(f"   ⚠️ XML parse error: {e}")
    return generation

# =============================================================================
# DATABASE FUNCTIONS
# =============================================================================

def insert_prices(prices):
    if not prices:
        return 0
    conn = get_db_connection()
    cursor = conn.cursor()
    
    insert_sql = """
    INSERT IGNORE INTO energy_prices 
    (`DateTime(UTC)`, AreaCode, AreaDisplayName, `Price[Currency/MWh]`, 
     ResolutionCode, Currency, ContractType, source_month)
    VALUES (%s, %s, %s, %s, %s, %s, 'Day-ahead', %s)
    """
    
    batch = []
    for p in prices:
        batch.append((
            p['datetime_utc'], p['area_code'], p['area_name'],
            p['price'], p['resolution'], p['currency'], p['month']
        ))
    
    cursor.executemany(insert_sql, batch)
    conn.commit()
    inserted = cursor.rowcount
    cursor.close()
    conn.close()
    return inserted

def insert_generation(generation_data):
    if not generation_data:
        return 0
    conn = get_db_connection()
    cursor = conn.cursor()
    
    insert_sql = """
    INSERT IGNORE INTO generation_per_type 
    (`DateTime(UTC)`, AreaCode, AreaDisplayName, ProductionType, 
     ActualGenerationOutput, ResolutionCode, source_month)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    batch = []
    for g in generation_data:
        batch.append((
            g['datetime'], g['area_code'], g['area_name'],
            g['production_type'], g['output'], g['resolution'], g['month']
        ))
    
    cursor.executemany(insert_sql, batch)
    conn.commit()
    inserted = cursor.rowcount
    cursor.close()
    conn.close()
    return inserted

# =============================================================================
# MAIN UPDATE FUNCTIONS
# =============================================================================

def daily_update():
    """Fetch last 31 days to catch any delays"""
    print("\n Daily Update: Fetching last 31 days")
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=31)
    
    total_prices = 0
    total_gen = 0
    
    for area_code, area_name in BIDDING_ZONES.items():
        print(f"   {area_name}...", end='', flush=True)
        
        # Fetch prices
        prices = fetch_day_ahead_prices(area_code, start_date, end_date)
        if prices:
            inserted = insert_prices(prices)
            total_prices += inserted
            print(f" P:{inserted}", end='', flush=True)
        
        # Fetch generation (Solar only for now)
        gen_data = fetch_generation(area_code, 'B16', start_date, end_date)
        if gen_data:
            inserted = insert_generation(gen_data)
            total_gen += inserted
            print(f" G:{inserted}", end='', flush=True)
        
        print()  # New line
        time.sleep(0.3)  # Rate limiting
    
    print(f"\n   Total: {total_prices} price records, {total_gen} generation records")

rebuild_summary_daily(start_date, end_date)
recalculate_summaries()

# =============================================================================
# MAIN
# =============================================================================

def main():
    if not ENTSOE_API_TOKEN:
        print("❌ ENTSOE_API_TOKEN environment variable not set")
        sys.exit(1)
    
    print("=" * 70)
    print("ENTSO-E Daily Update Script")
    print(f"Started at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 70)
    
    try:
        daily_update()
        print("\n✅ Update complete!")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

