import requests
import pandas as pd
import logging
import time
from airflow.exceptions import AirflowException
# ingest logic (v2)
def ingest_taxi_data(**context):
    url = "https://data.cityofnewyork.us/api/views/t29m-gskq/rows.csv"
    
    output_path = "/opt/airflow/dags/nyc_taxi_raw.csv" 
    
    max_rows = 10000 
    
    for attempt in range(3):
        try:
            logging.info(f"Attempt {attempt + 1}: Streaming download from {url}")
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()
            
            with open(output_path, "wb") as f:
                lines_written = 0
                for line in response.iter_lines():
                    if line is not None:
                        f.write(line + b'\n')
                        lines_written += 1
                        
                        # ถ้าเขียนครบ 10,000 บรรทัด ให้หยุดดาวน์โหลดทันที
                        if lines_written > max_rows:
                            logging.info(f"Reached {max_rows} rows limit. Stopping stream early to save time.")
                            response.close()
                            break
            
            logging.info("Download finished successfully.")
            break
            
        except requests.exceptions.RequestException as e:
            logging.warning(f"Download attempt {attempt + 1} failed: {e}")
            if attempt == 2:
                raise AirflowException(f"Failed to download taxi data after 3 attempts: {e}")
            time.sleep(5)
            
    logging.info("Validating row count with pandas...")
    df_chunk = pd.read_csv(output_path, usecols=[0])
    row_count = len(df_chunk)
    
    if row_count < 1000:
        raise AirflowException(f"Data validation failed: Expected at least 1000 rows, but found only {row_count} rows.")
        
    logging.info(f"Validation passed. Successfully downloaded {row_count:,} rows.")
    context["ti"].xcom_push(key="raw_path", value=output_path)
    
    return output_path