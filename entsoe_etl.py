import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from psycopg2.extras import execute_values
import psycopg2
import pytz

# --- Read secrets from environment variables ---
SECURITY_TOKEN = os.environ.get("ENTSOE_TOKEN")

AZURE_PG_HOST = os.environ.get("AZURE_PG_HOST")        
AZURE_PG_DB = os.environ.get("AZURE_PG_DB")            
AZURE_PG_USER = os.environ.get("AZURE_PG_USER")        
AZURE_PG_PASSWORD = os.environ.get("AZURE_PG_PASSWORD") 

# --- Define countries for future extension ---
countries = {
    "Germany": "10YDE-RWENET---I",
    # Add other countries here, e.g.,
    # "Italy North": "10Y1001A1001A82H",
}

# --- ETL function ---
def fetch_and_store_data(period_start, period_end, country_name, control_area):
    API_URL = "https://web-api.tp.entsoe.eu/api"
    PARAMS = {
        "securityToken": SECURITY_TOKEN,
        "documentType": "A81",
        "businessType": "B95",
        "processType": "A52",
        "Type_MarketAgreement.Type": "A01",
        "controlArea_Domain": control_area,
        "periodStart": period_start,
        "periodEnd": period_end
    }

    response = requests.get(API_URL, params=PARAMS)
    response.raise_for_status()
    xml_data = response.content

    root = ET.fromstring(xml_data)
    ns = {'ns': root.tag.split('}')[0].strip('{')}

    direction_map = {"A01": "Up", "A02": "Down", "A03": "Up and Down (Symmetric)"}
    cet = pytz.timezone("Europe/Berlin")

    data = []

    for ts in root.findall(".//ns:TimeSeries", ns):
        reserve_type = ts.find("ns:type_MarketAgreement.type", ns)
        reserve_type = reserve_type.text if reserve_type is not None else None
        reserve_source = ts.find("ns:mktPSRType.psrType", ns)
        reserve_source = reserve_source.text if reserve_source is not None else None
        direction_code = ts.find("ns:flowDirection.direction", ns)
        direction_code = direction_code.text if direction_code is not None else None
        direction = direction_map.get(direction_code, direction_code)
        product_type = ts.find("ns:standard_MarketProduct.marketProductType", ns)
        product_type = product_type.text if product_type is not None else None
        time_horizon = ts.find("ns:type_MarketAgreement.type", ns)
        time_horizon = time_horizon.text if time_horizon is not None else None
        price_type = "Marginal"

        for period in ts.findall("ns:Period", ns):
            start_time_str = period.find("ns:timeInterval/ns:start", ns).text
            resolution = period.find("ns:resolution", ns).text
            start_time = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=pytz.utc).astimezone(cet)

            if resolution == "PT15M":
                delta = timedelta(minutes=15)
            elif resolution == "PT30M":
                delta = timedelta(minutes=30)
            else:
                delta = timedelta(hours=1)

            for point in period.findall("ns:Point", ns):
                quantity_el = point.find("ns:quantity", ns)
                price_el = point.find("ns:procurement_Price.amount", ns)
                
                # Null-safe: default to 0 if missing
                quantity = float(quantity_el.text) if quantity_el is not None else 0
                price = float(price_el.text) if price_el is not None else 0

                end_time = start_time + delta
                delivery_period = f"{start_time.strftime('%d.%m.%Y %H:%M')} - {end_time.strftime('%d.%m.%Y %H:%M')} (CET/CEST)"

                data.append({
                    "delivery_period": delivery_period,
                    "reserve_type": reserve_type,
                    "reserve_source": reserve_source,
                    "direction": direction,
                    "volume": quantity,
                    "price": price,
                    "price_type": price_type,
                    "type_of_product": product_type,
                    "time_horizon": time_horizon,
                    "country": country_name
                })
                start_time += delta

    if not data:
        print(f"No data returned for {country_name} {period_start} - {period_end}")
        return

    df = pd.DataFrame(data)

    # --- Connect to Azure PostgreSQL ---
    conn = psycopg2.connect(
        dbname=AZURE_PG_DB,
        user=AZURE_PG_USER,
        password=AZURE_PG_PASSWORD,
        host=AZURE_PG_HOST,
        port=5432,
        sslmode='require'
    )
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS entsoe_load_data (
        delivery_period TEXT,
        reserve_type TEXT,
        reserve_source TEXT,
        direction TEXT,
        volume DOUBLE PRECISION,
        price DOUBLE PRECISION,
        price_type TEXT,
        type_of_product TEXT,
        time_horizon TEXT,
        country TEXT,
        inserted_at TIMESTAMP DEFAULT now()
    )
    """)
    conn.commit()

    records = df.to_dict("records")
    columns = df.columns.tolist()
    values = [[r.get(col) for col in columns] for r in records]

    execute_values(
        cursor,
        f"INSERT INTO entsoe_load_data ({', '.join(columns)}) VALUES %s",
        values
    )
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Inserted {len(df)} rows for {country_name} {period_start} - {period_end}")

# --- Historical load ---
def historical_load():
    start_date = datetime(2024, 1, 1)
    end_date = datetime.utcnow()
    current = start_date

    while current < end_date:
        period_start = current.strftime('%Y%m%d%H%M')
        period_end = (current + timedelta(days=30)).strftime('%Y%m%d%H%M')
        for country_name, control_area in countries.items():
            fetch_and_store_data(period_start, period_end, country_name, control_area)
        current += timedelta(days=30)

# --- Daily load ---
def daily_load():
    yesterday = datetime.utcnow() - timedelta(days=1)
    period_start = yesterday.strftime('%Y%m%d0000')
    period_end = yesterday.strftime('%Y%m%d2300')
    for country_name, control_area in countries.items():
        fetch_and_store_data(period_start, period_end, country_name, control_area)

# --- Entry point ---
if __name__ == "__main__":
    # Uncomment for historical load once
    # historical_load()

    # Daily load
    daily_load()
