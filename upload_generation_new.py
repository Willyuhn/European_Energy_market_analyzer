"""
Upload new Generation per Type data to Google Cloud SQL
- Reads from Generation_per_type_new/ folder
- Filters for Solar and Wind only
- Replaces old data in generation_per_type table
"""

import mysql.connector
import csv
import os
import sys
from pathlib import Path

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)

# Database configuration
DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "port": int(os.environ.get("DB_PORT", "3306")),
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "database": os.environ.get("DB_NAME", "energy_market"),
    "use_pure": True,
    "connection_timeout": 300,
    'autocommit': False,
}

# Path to new generation files
DATA_DIR = Path(__file__).parent / "Generation_per_type_new"

# Production types to keep
KEEP_TYPES = ['Solar', 'Wind Onshore', 'Wind Offshore']

BATCH_SIZE = 1000  # Smaller batches for stability


def get_connection():
    """Get database connection"""
    print("   Attempting connection...")
    conn = mysql.connector.connect(**DB_CONFIG)
    print("   Connection established!")
    return conn


def recreate_table(cursor):
    """Drop and recreate the generation_per_type table with correct schema"""
    print("📋 Recreating generation_per_type table...")
    
    print("   Dropping old table...")
    cursor.execute("DROP TABLE IF EXISTS generation_per_type")
    print("   Old table dropped.", flush=True)
    
    print("   Creating new table...", flush=True)
    cursor.execute("""
        CREATE TABLE generation_per_type (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            `DateTime(UTC)` VARCHAR(50) NOT NULL,
            ResolutionCode VARCHAR(20),
            AreaCode VARCHAR(50) NOT NULL,
            AreaDisplayName VARCHAR(100),
            AreaTypeCode VARCHAR(20),
            AreaMapCode VARCHAR(20),
            ProductionType VARCHAR(100) NOT NULL,
            ActualGenerationOutput DECIMAL(14, 4),
            ActualConsumption DECIMAL(14, 4),
            `UpdateTime(UTC)` VARCHAR(50),
            source_month TINYINT NOT NULL,
            
            INDEX idx_datetime (`DateTime(UTC)`),
            INDEX idx_area_code (AreaCode),
            INDEX idx_area_display (AreaDisplayName),
            INDEX idx_production_type (ProductionType),
            INDEX idx_month (source_month),
            INDEX idx_area_datetime (AreaCode, `DateTime(UTC)`),
            INDEX idx_solar (ProductionType, ActualGenerationOutput)
        ) ENGINE=InnoDB
    """)
    print("   ✅ Table created", flush=True)


def create_table_fresh(conn):
    """Create table with its own cursor and commit"""
    cursor = conn.cursor()
    
    print("   Dropping old table...", flush=True)
    cursor.execute("DROP TABLE IF EXISTS generation_per_type")
    conn.commit()
    print("   Old table dropped.", flush=True)
    
    print("   Creating new table...", flush=True)
    cursor.execute("""
        CREATE TABLE generation_per_type (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            `DateTime(UTC)` VARCHAR(50) NOT NULL,
            ResolutionCode VARCHAR(20),
            AreaCode VARCHAR(50) NOT NULL,
            AreaDisplayName VARCHAR(100),
            AreaTypeCode VARCHAR(20),
            AreaMapCode VARCHAR(20),
            ProductionType VARCHAR(100) NOT NULL,
            ActualGenerationOutput DECIMAL(14, 4),
            ActualConsumption DECIMAL(14, 4),
            `UpdateTime(UTC)` VARCHAR(50),
            source_month TINYINT NOT NULL,
            
            INDEX idx_datetime (`DateTime(UTC)`),
            INDEX idx_area_code (AreaCode),
            INDEX idx_area_display (AreaDisplayName),
            INDEX idx_production_type (ProductionType),
            INDEX idx_month (source_month)
        ) ENGINE=InnoDB
    """)
    conn.commit()
    cursor.close()
    print("   ✅ Table created and committed", flush=True)


def parse_float(value):
    """Parse float value, return None for empty strings"""
    if value is None or value == '' or value == 'n/e':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def upload_file(cursor, conn, filepath, month):
    """Upload a single CSV file, filtering for Solar/Wind only"""
    filename = os.path.basename(filepath)
    print(f"   📁 Processing {filename}...")
    
    rows_to_insert = []
    total_rows = 0
    filtered_rows = 0
    
    with open(filepath, 'r', encoding='utf-8') as f:
        # Detect delimiter (tab or comma)
        first_line = f.readline()
        f.seek(0)
        delimiter = '\t' if '\t' in first_line else ','
        
        reader = csv.DictReader(f, delimiter=delimiter)
        
        for row in reader:
            total_rows += 1
            
            # Filter for Solar and Wind only
            production_type = row.get('ProductionType', '')
            if production_type not in KEEP_TYPES:
                continue
            
            filtered_rows += 1
            
            # Prepare row data
            row_data = (
                row.get('DateTime(UTC)', ''),
                row.get('ResolutionCode', ''),
                row.get('AreaCode', ''),
                row.get('AreaDisplayName', ''),
                row.get('AreaTypeCode', ''),
                row.get('AreaMapCode', ''),
                production_type,
                parse_float(row.get('ActualGenerationOutput[MW]')),
                parse_float(row.get('ActualConsumption[MW]')),
                row.get('UpdateTime(UTC)', ''),
                month
            )
            
            rows_to_insert.append(row_data)
            
            # Batch insert
            if len(rows_to_insert) >= BATCH_SIZE:
                insert_batch(cursor, rows_to_insert)
                conn.commit()
                print(f"      ... inserted {filtered_rows:,} rows so far", flush=True)
                rows_to_insert = []
    
    # Insert remaining rows
    if rows_to_insert:
        insert_batch(cursor, rows_to_insert)
        conn.commit()
    
    print(f"      Total: {total_rows:,} rows, Kept (Solar/Wind): {filtered_rows:,} rows")
    return filtered_rows


def insert_batch(cursor, rows):
    """Insert a batch of rows"""
    if not rows:
        return
    
    query = """
        INSERT INTO generation_per_type 
        (`DateTime(UTC)`, ResolutionCode, AreaCode, AreaDisplayName, 
         AreaTypeCode, AreaMapCode, ProductionType, 
         ActualGenerationOutput, ActualConsumption, `UpdateTime(UTC)`, source_month)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    cursor.executemany(query, rows)


def main():
    print("=" * 60, flush=True)
    print("Upload New Generation Data to Google Cloud SQL", flush=True)
    print("=" * 60, flush=True)
    print(flush=True)
    print(f"Data source: {DATA_DIR}", flush=True)
    print(f"Filtering for: {', '.join(KEEP_TYPES)}", flush=True)
    print(flush=True)
    
    # Get list of CSV files
    csv_files = sorted(DATA_DIR.glob("*.csv"))
    
    if not csv_files:
        print("❌ No CSV files found in Generation_per_type_new/")
        return
    
    print(f"Found {len(csv_files)} files to process")
    print()
    
    # Connect to database
    print("🔌 Connecting to Google Cloud SQL...")
    conn = get_connection()
    cursor = conn.cursor()
    print("   ✅ Connected")
    print()
    
    # Recreate table (using separate function with proper commits)
    create_table_fresh(conn)
    print()
    
    # Get fresh cursor for inserts
    cursor = conn.cursor()
    
    # Upload each file
    print("📤 Uploading data...")
    total_inserted = 0
    
    for filepath in csv_files:
        # Extract month from filename (e.g., 2025_01_... -> 1)
        filename = filepath.name
        try:
            month = int(filename.split('_')[1])
        except (IndexError, ValueError):
            print(f"   ⚠️  Could not parse month from {filename}, skipping")
            continue
        
        rows = upload_file(cursor, conn, filepath, month)
        total_inserted += rows
    
    print()
    print("=" * 60)
    print(f"✅ Upload complete!")
    print(f"   Total rows inserted: {total_inserted:,}")
    print("=" * 60)
    
    # Verify
    print()
    print("📊 Verification:")
    cursor.execute("SELECT COUNT(*) FROM generation_per_type")
    count = cursor.fetchone()[0]
    print(f"   Total rows in table: {count:,}")
    
    cursor.execute("""
        SELECT ProductionType, COUNT(*) as cnt 
        FROM generation_per_type 
        GROUP BY ProductionType
    """)
    print("   By production type:")
    for row in cursor.fetchall():
        print(f"      {row[0]}: {row[1]:,}")
    
    cursor.execute("""
        SELECT source_month, COUNT(*) as cnt 
        FROM generation_per_type 
        GROUP BY source_month 
        ORDER BY source_month
    """)
    print("   By month:")
    for row in cursor.fetchall():
        print(f"      Month {row[0]}: {row[1]:,}")
    
    cursor.close()
    conn.close()
    print()
    print("🎉 Done!")


if __name__ == "__main__":
    main()

