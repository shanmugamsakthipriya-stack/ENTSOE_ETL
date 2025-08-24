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
SUPABASE_HOST = os.environ.get("SUPABASE_HOST")        # e.g., db.igtsnbkybiyrtkrqxpco.supabase.co
SUPABASE_DB = os.environ.get("SUPABASE_DB")            # e.g., postgres
SUPABASE_USER = os.environ.get("SUPABASE_USER")        # e.g., postgres
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")  # service role key

# --- ENTSOE API Call ---
API_URL = "https://web-api.tp.entsoe.eu/api"
PARAMS = {
    "securityToken": SECURITY_TOKEN,
    "documentType": "A81",
    "businessType": "B95",
    "processType": "A52",
    "Type_MarketAgreement.Type": "A01",
    "controlArea_Domain": "10YDE-RWENET---I",
    "periodStart": "202409242200",
    "periodEnd": "202409252200"
}

response = requests.get(API_URL, params=PARAMS)
response.raise_for_status()
xml_data = response.content

# --- Parse XML with namespace ---
root = ET.fromstring(xml_data)
ns = {'ns': root.tag.split('}')[0].strip('{')}

# --- Mapping dictionaries ---
direction_map = {
    "A01": "Up",
    "A02": "Down",
    "A03": "Up and Down (Symmetric)"
}

# --- Parse XML ---
data = []
cet = pytz.timezone("Europe/Berlin")  # CET/CEST with DST handled

for ts in root.findall(".//ns:TimeSeries", ns):
    reserve_type = ts.find("ns:type_MarketAgreement.type", ns).text if ts.find("ns:type_MarketAgreement.type", ns) is not None else None
    reserve_source = ts.find("ns:mktPSRType.psrType", ns).text if ts.find("ns:mktPSRType.psrType", ns) is not None else None
    direction_code = ts.find("ns:flowDirection.direction", ns).text if ts.find("ns:flowDirection.direction", ns) is not None else None
    direction = direction_map.get(direction_code, direction_code)
    product_type = ts.find("ns:standard_MarketProduct.marketProductType", ns).text if ts.find("ns:standard_MarketProduct.marketProductType", ns) is not None else None
    time_horizon = ts.find("ns:type_MarketAgreement.type", ns).text if ts.find("ns:type_MarketAgreement.type", ns) is not None else None
    price_type = "Marginal"

    for period in ts.findall("ns:Period", ns):
        start_time_str = period.find("ns:timeInterval/ns:start", ns).text
        resolution = period.find("ns:resolution", ns).text
        start_time = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=pytz.utc).astimezone(cet)

        # Determine interval delta
        if resolution == "PT15M":
            delta = timedelta(minutes=15)
        elif resolution == "PT30M":
            delta = timedelta(minutes=30)
        else:
            delta = timedelta(hours=1)

        for point in period.findall("ns:Point", ns):
            quantity_el = point.find("ns:quantity", ns)
            price_el = point.find("ns:procurement_Price.amount", ns)
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
                "time_horizon": time_horizon
            })
            start_time += delta

# --- Create DataFrame ---
df = pd.DataFrame(data)
print(df.head())

# --- Connect to Supabase PostgreSQL using service role key ---
conn = psycopg2.connect(
    dbname=SUPABASE_DB,
    user=SUPABASE_USER,
    password=SUPABASE_SERVICE_KEY,
    host=SUPABASE_HOST,
    port=5432,
    sslmode='require'
)
cursor = conn.cursor()

# --- Create table if not exists ---
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
    time_horizon TEXT
)
""")
conn.commit()

# --- Insert data ---
records = df.to_dict("records")
columns = df.columns.tolist()
values = [[r[col] for col in columns] for r in records]

execute_values(
    cursor,
    f"INSERT INTO entsoe_load_data ({', '.join(columns)}) VALUES %s",
    values
)
conn.commit()
cursor.close()
conn.close()

print("Data successfully stored in Supabase PostgreSQL!")
