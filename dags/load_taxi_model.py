import pandas as pd
import logging
from sqlalchemy import create_engine
from airflow.sdk import Variable   

def load_taxi_model(**context):
    # 1. Pull path จาก XCom
    transformed_path = context["ti"].xcom_pull(
        key="transformed_path",
        task_ids="transform_taxi_data"
    )
    
    if not transformed_path:
        raise ValueError("No transformed_path found in XCom.")

    logging.info(f"Loading transformed data from: {transformed_path}")
    
    # 2. โหลดข้อมูล
    df = pd.read_csv(transformed_path)

    # ==========================================
    # ✅ 3. CONNECT MYSQL connect
    # ==========================================
    mysql_host = "mydb"        
    mysql_user = "admin"      
    mysql_pass = "secret"
    mysql_db   = "homestead"

    conn_str = f"mysql+pymysql://{mysql_user}:{mysql_pass}@{mysql_host}/{mysql_db}"
    engine = create_engine(conn_str)

    logging.info("Building Star Schema...")

    # ==========================================
    # 4. CREATE DIM TABLES
    # ==========================================

    # dim_time
    dim_time = df[["pickup_hour", "pickup_day_of_week", "is_weekend"]] \
        .drop_duplicates().reset_index(drop=True)
    dim_time["time_id"] = dim_time.index + 1

    # dim_payment
    dim_payment = df[["payment_type"]] \
        .drop_duplicates().dropna().reset_index(drop=True)
    dim_payment["payment_id"] = dim_payment.index + 1

    # ==========================================
    # 5. CREATE FACT TABLE
    # ==========================================
    fact_df = df.merge(
        dim_time,
        on=["pickup_hour", "pickup_day_of_week", "is_weekend"],
        how="left"
    )

    fact_df = fact_df.merge(
        dim_payment,
        on=["payment_type"],
        how="left"
    )

    fact_columns = [
        "time_id", "payment_id", "fare_amount", "trip_distance",
        "trip_duration_minutes", "speed_mph",
        "fare_per_mile", "passenger_count"
    ]

    fact_trips = fact_df[fact_columns]

    # ==========================================
    # 6. LOAD TO MYSQL
    # ==========================================
    logging.info("Loading data into MySQL database...")

    dim_time.to_sql("dim_time", con=engine, if_exists="replace", index=False, chunksize=1000)
    dim_payment.to_sql("dim_payment", con=engine, if_exists="replace", index=False, chunksize=1000)
    fact_trips.to_sql("fact_trips", con=engine, if_exists="replace", index=False, chunksize=1000)

    # ==========================================
    # 7. LOG RESULT
    # ==========================================
    logging.info(f"Loaded {len(dim_time)} rows → dim_time")
    logging.info(f"Loaded {len(dim_payment)} rows → dim_payment")
    logging.info(f"Loaded {len(fact_trips)} rows → fact_trips")

    return "Database load completed successfully."