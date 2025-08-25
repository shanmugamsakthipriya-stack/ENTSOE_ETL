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

# --- Define countries for future extension ---
countries = {
    "Germany": "10YDE-RWENET---I",
    # Add other countries here
}

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
            logging.warning(f"No data returned for {country_name} {period_start} - {period_end}")
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
            "periodEnd": period_end
        }

        response = requests.get(API_URL, params=PARAMS)
        response.raise_for_status()
        xml_data = response.content

        root = ET.fromstring(xml_data)
        ns = {'ns': root.tag.split('}')[0].strip('{')}

        data = []
        cet = pytz.timezone("Europe/Berlin")

        for ts in root.findall(".//ns:TimeSeries", ns):
            start_time_str = ts.find("ns:Period/ns:timeInterval/ns:start", ns).text
            start_time = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=pytz.utc).astimezone(cet)

            for point in ts.findall(".//ns:Point", ns):
                position = int(point.find("ns:position", ns).text)
                price = float(point.find("ns:price.amount", ns).text)

                mtu_start = start_time + timedelta(minutes=15 * (position - 1))
                mtu_end = mtu_start + timedelta(minutes=15)

                data.append({
                    "mtu": f"{mtu_start.strftime('%d.%m.%Y %H:%M')} - {mtu_end.strftime('%d.%m.%Y %H:%M')} (CET/CEST)",
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
            mtu TEXT,
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
        # Balancing reserve ETL
        fetch_and_store_data(period_start, period_end, country_name, control_area)
        # Day-ahead price ETL (use bidding zone same as control_area if needed)
        fetch_and_store_dayahead_prices(period_start, period_end, country_name, control_area)

# --- Entry point ---
if __name__ == "__main__":
    try:
        if '--historical' in sys.argv:
            logging.info("Starting historical load...")
            historical_load()
        else:
            logging.info("Starting daily load...")
            daily_load()
        logging.info("ETL completed successfully.")
    except Exception as e:
        logging.error(f"ETL failed: {e}", exc_info=True)
        send_email_alert("ENTSOE ETL Failed", str(e))
        sys.exit(1)
