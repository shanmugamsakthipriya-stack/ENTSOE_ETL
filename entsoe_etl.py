import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from psycopg2.extras import execute_values
import psycopg2
import pytz
import sys
import logging
import smtplib
from email.mime.text import MIMEText
import requests as req
import argparse

# --- Logging configuration ---
logging.basicConfig(
    filename='entsoe_etl.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)

# --- Email alert function ---
def send_email_alert(subject, message):
    try:
        smtp_server = os.environ.get("SMTP_SERVER")
        smtp_port = os.environ.get("SMTP_PORT", 587)
        smtp_user = os.environ.get("SMTP_USER")
        smtp_password = os.environ.get("SMTP_PASSWORD")
        to_email = os.environ.get("ALERT_EMAIL")

        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = smtp_user
        msg['To'] = to_email

        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, [to_email], msg.as_string())
        server.quit()
        logging.info("Alert email sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send alert email: {e}")

# --- Read secrets from environment variables ---
SECURITY_TOKEN = os.environ.get("ENTSOE_TOKEN")
AZURE_PG_HOST = os.environ.get("AZURE_PG_HOST")        
AZURE_PG_DB = os.environ.get("AZURE_PG_DB")            
AZURE_PG_USER = os.environ.get("AZURE_PG_USER")        
AZURE_PG_PASSWORD = os.environ.get("AZURE_PG_PASSWORD") 

# Updated Germany TSOs
germany_control_areas = {
    "TransnetBW": "10YDE-ENBW-----N",
    "TenneT": "10YDE-EON------1",
    "Amprion": "10YDE-RWENET---I",
    "50Hertz": "10YDE-VEEN------N"
}
germany_bidding_zone = "10Y1001A1001A82H"  # DE-LU BZN

# --- Helper to compute start/end times ---
def get_time_interval(start_time, resolution, position):
    if resolution == "PT15M":
        delta = timedelta(minutes=15)
    elif resolution == "PT30M":
        delta = timedelta(minutes=30)
    else:
        delta = timedelta(hours=1)
    point_start = start_time + delta * (position - 1)
    point_end = point_start + delta
    return point_start, point_end
    
# --- ETL function ---
def fetch_and_store_data(period_start, period_end, country_name, control_area):
    try:
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
                start_time = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=pytz.utc).astimezone(cet)
                
                resolution = period.find("ns:resolution", ns).text

                for point in period.findall("ns:Point", ns):
                    quantity_el = point.find("ns:quantity", ns)
                    price_el = point.find("ns:procurement_Price.amount", ns)
                    quantity = float(quantity_el.text) if quantity_el is not None else 0
                    price = float(price_el.text) if price_el is not None else 0
                    
                    # Compute timestamp for this point
                    position_el = point.find("ns:position", ns)
                    position = int(position_el.text) if position_el is not None else 1
                    
                    point_start, point_end = get_time_interval(start_time, resolution, position)
                    
                    delivery_period = f"{point_start.strftime('%d.%m.%Y %H:%M')} - {point_end.strftime('%d.%m.%Y %H:%M')} (CET/CEST)"
                    
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
                        "country": country_name,
                        "control_area": control_area 
                    })

        if not data:
            logging.warning(f"No data returned for {country_name} {control_area} {period_start} - {period_end}")
            return

        df = pd.DataFrame(data)

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
            time_horizon TEXT
        )
        """)
        conn.commit()

        # Self-healing columns
        required_columns = {
            "country": "TEXT",
            "control_area": "TEXT",
            "inserted_at": "TIMESTAMP DEFAULT now()"
        }
        for col, col_type in required_columns.items():
            cursor.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='entsoe_load_data' AND column_name='{col}'
                    ) THEN
                        ALTER TABLE entsoe_load_data ADD COLUMN {col} {col_type};
                    END IF;
                END
                $$;
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
        logging.info(f"Inserted {len(df)} rows for {country_name} {period_start} - {period_end}")

    except Exception as e:
        logging.error(f"ETL failed for {country_name} {period_start} - {period_end}: {e}", exc_info=True)
        send_email_alert("ENTSOE ETL Failed", f"{country_name} {period_start}-{period_end}\n{e}")
        raise
        
def fetch_and_store_dayahead_prices(period_start, period_end, country_name, bidding_zone):
    try:
        API_URL = "https://web-api.tp.entsoe.eu/api"
        PARAMS = {
            "securityToken": SECURITY_TOKEN,
            "documentType": "A44",   # Day-ahead prices
            "in_Domain": bidding_zone,
            "out_Domain": bidding_zone,
            "periodStart": period_start,
            "periodEnd": period_end,
            "contract_MarketAgreement.type": "A01"
        }

        response = requests.get(API_URL, params=PARAMS)
        response.raise_for_status()
        xml_data = response.content

        root = ET.fromstring(xml_data)
        ns = {'ns': root.tag.split('}')[0].strip('{')}

        data = []
        cet = pytz.timezone("Europe/Berlin")

        for ts in root.findall(".//ns:TimeSeries", ns):
            for period in ts.findall(".//ns:Period", ns):
                start_time_str = period.find("ns:timeInterval/ns:start", ns).text
                start_time = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=pytz.utc).astimezone(cet)
                resolution = period.find("ns:resolution", ns).text
                
                for point in period.findall("ns:Point",ns):
                    position_el = point.find("ns:position", ns)
                    price_el = point.find("ns:price.amount", ns)
                    if position_el is None or price_el is None:
                        continue

                    position = int(position_el.text)
                    price = float(price_el.text)

                    mtu_start, mtu_end = get_time_interval(start_time, resolution, position)
                    
                    data.append({
                    #"mtu": f"{mtu_start.strftime('%d.%m.%Y %H:%M')} - {mtu_end.strftime('%d.%m.%Y %H:%M')} (CET/CEST)",
                    "mtu_start": mtu_start,
                    "mtu_end": mtu_end, 
                    "price_eur_mwh": price,
                    "country": country_name
                })

        if not data:
            logging.warning(f"No Day-ahead data for {country_name} {period_start} - {period_end}")
            return

        df = pd.DataFrame(data)

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
        CREATE TABLE IF NOT EXISTS day_ahead_prices (
            mtu_start TIMESTAMP,
            mtu_end TIMESTAMP,
            price_eur_mwh DOUBLE PRECISION,
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
            f"INSERT INTO day_ahead_prices ({', '.join(columns)}) VALUES %s",
            values
        )
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Inserted {len(df)} Day-ahead price rows for {country_name} {period_start} - {period_end}")

    except Exception as e:
        logging.error(f"Day-ahead ETL failed for {country_name} {period_start} - {period_end}: {e}", exc_info=True)
        send_email_alert("ENTSOE Day-ahead ETL Failed", f"{country_name} {period_start}-{period_end}\n{e}")
        raise

# --- Historical load ---
def historical_load():
    start_date = datetime(2024, 1, 1)
    end_date = datetime.utcnow()
    current = start_date
    interval_days = 30
    logging.info(f"Starting historical load from {start_date.date()} to {end_date.date()}")
    
    while current < end_date:
        period_start = current.strftime('%Y%m%d%H%M')
        period_end_dt = min(current + timedelta(days=interval_days), end_date)
        period_end = period_end_dt.strftime('%Y%m%d%H%M')
        logging.info(f"Processing interval: {period_start} - {period_end}")
        
        # Balancing reserves per TSO
        for tso_name, control_area in germany_control_areas.items():
            logging.info(f"Fetching data for {tso_name}")
            fetch_and_store_data(period_start, period_end, f"Germany-{tso_name}", control_area)
            
        # Day-ahead prices per bidding zone
        logging.info(f"Fetching day-ahead prices for {germany_bidding_zone}")
        fetch_and_store_dayahead_prices(period_start, period_end, "BZN|DE-LU", germany_bidding_zone)
        
        current += timedelta(days=interval_days)
        logging.info(f"Completed interval up to {period_end}")

# --- Daily load ---
def daily_load():
    y = datetime.utcnow() - timedelta(days=1)
    period_start = y.strftime('%Y%m%d0000')
    period_end = (y + timedelta(days=1)).strftime('%Y%m%d0000')
    
    for tso_name, control_area in germany_control_areas.items():
        # Balancing reserve ETL
        fetch_and_store_data(period_start, period_end, f"Germany-{tso_name}", control_area)
        
    # Day-ahead prices per bidding zone
    fetch_and_store_dayahead_prices(period_start, period_end, "BZN|DE-LU", germany_bidding_zone)

# --- Entry point ---
if __name__ == "__main__":
    try:
        # Check if historical has been run
        last_run_file = ".last_historical_run"
        if not os.path.exists(last_run_file):
            logging.info("Running historical backfill from Jan 1, 2024...")
            historical_load()
            with open(last_run_file, "w") as f:
                f.write(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))

        logging.info("Running daily load...")
        daily_load()

        logging.info("ETL completed successfully.")

    except Exception as e:
        logging.error(f"ETL failed: {e}", exc_info=True)
        send_email_alert("ENTSOE ETL Failed", str(e))
        sys.exit(1)
