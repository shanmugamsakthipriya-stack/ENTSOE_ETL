import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values

# --------------------------
# CONFIGURATION
# --------------------------
API_URL = "https://web-api.tp.entsoe.eu/api"
SECURITY_TOKEN = "b99f6903-7ec9-48f2-9cb0-e8359aa76b3a"

# PostgreSQL config
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "Friday@22",
    "host": "localhost",
    "port": "5432"
}

TABLE_NAME = "entsoe_reserves"

# Timezone
CET = pytz.timezone("CET")

# --------------------------
# HELPER FUNCTIONS
# --------------------------

def fetch_entsoe_data(start_period, end_period, document_type="A81", business_type="B95",
                      process_type="A52", market_agreement="A01", area_domain="10YDE-RWENET---I"):
    """
    Fetch XML from ENTSOE API and return parsed DataFrame
    """
    params = {
        "securityToken": SECURITY_TOKEN,
        "documentType": document_type,
        "businessType": business_type,
        "processType": process_type,
        "Type_MarketAgreement.Type": market_agreement,
        "controlArea_Domain": area_domain,
        "periodStart": start_period.strftime("%Y%m%d%H%M"),
        "periodEnd": end_period.strftime("%Y%m%d%H%M")
    }

    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    xml_data = response.content

    # Parse XML
    root = ET.fromstring(xml_data)
    ns = {'ns': root.tag.split('}')[0].strip('{')}

    data = []
    for ts in root.findall(".//ns:TimeSeries", ns):
        # Optional fields with fallback
        reserve_type = ts.find("ns:type_MarketAgreement.type", ns)
        reserve_type = reserve_type.text if reserve_type is not None else "Unknown"

        reserve_source = ts.find("ns:mktPSRType.psrType", ns)
        reserve_source = reserve_source.text if reserve_source is not None else "Unknown"

        direction = ts.find("ns:flowDirection.direction", ns)
        direction_map = {"A01": "Up", "A02": "Down", "A03": "Up and Down (Symmetric)"}
        direction = direction_map.get(direction.text if direction is not None else "", "Unknown")

        type_of_product = ts.find("ns:standard_MarketProduct.marketProductType", ns)
        type_of_product = type_of_product.text if type_of_product is not None else "Unknown"

        time_horizon = ts.find("ns:type_MarketAgreement.type", ns)
        time_horizon = time_horizon.text if time_horizon is not None else "Unknown"

        for period in ts.findall("ns:Period", ns):
            start_str = period.find("ns:timeInterval/ns:start", ns).text
            end_str = period.find("ns:timeInterval/ns:end", ns).text
            resolution = period.find("ns:resolution", ns).text

            start_dt = datetime.strptime(start_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=pytz.utc).astimezone(CET)
            end_dt = datetime.strptime(end_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=pytz.utc).astimezone(CET)

            for point in period.findall("ns:Point", ns):
                position = int(point.find("ns:position", ns).text)
                volume = float(point.find("ns:quantity", ns).text)
                price_node = point.find("ns:procurement_Price.amount", ns)
                price = float(price_node.text) if price_node is not None else 0.0
                price_type_node = point.find("ns:imbalance_Price.category", ns)
                price_type = price_type_node.text if price_type_node is not None else "Unknown"

                delivery_period = f"{(start_dt + timedelta(minutes=15*(position-1))).strftime('%d.%m.%Y %H:%M')} - {(start_dt + timedelta(minutes=15*position)).strftime('%d.%m.%Y %H:%M')} (CET/CEST)"

                data.append({
                    "delivery_period": delivery_period,
                    "reserve_type": reserve_type,
                    "reserve_source": reserve_source,
                    "direction": direction,
                    "volume": volume,
                    "price": price,
                    "price_type": price_type,
                    "type_of_product": type_of_product,
                    "time_horizon": time_horizon
                })

    df = pd.DataFrame(data)
    # Handle nulls safely
    df.fillna({
        "reserve_type": "Unknown",
        "reserve_source": "Unknown",
        "direction": "Unknown",
        "volume": 0,
        "price": 0.0,
        "price_type": "Unknown",
        "type_of_product": "Unknown",
        "time_horizon": "Unknown"
    }, inplace=True)

    return df

def store_to_postgres(df):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    # Create table if not exists
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            delivery_period TEXT,
            reserve_type TEXT,
            reserve_source TEXT,
            direction TEXT,
            volume DOUBLE PRECISION,
            price DOUBLE PRECISION,
            price_type TEXT,
            type_of_product TEXT,
            time_horizon TEXT
        )
    """)
    conn.commit()

    records = df.to_dict("records")
    columns = df.columns.tolist()
    values = [[r[col] for col in columns] for r in records]

    execute_values(
        cursor,
        f"INSERT INTO {TABLE_NAME} ({', '.join(columns)}) VALUES %s",
        values
    )
    conn.commit()
    cursor.close()
    conn.close()
    print(f"{len(df)} rows inserted into PostgreSQL table '{TABLE_NAME}'.")

# --------------------------
# MAIN SCRIPT
# --------------------------
if __name__ == "__main__":
    # Example: historical run for 23.09.2024
    start_date = datetime(2024, 9, 23, 0, 0)
    end_date = datetime(2024, 9, 24, 0, 0)
    
    df = fetch_entsoe_data(start_date, end_date)
    store_to_postgres(df)
    print("ETL run complete!")
