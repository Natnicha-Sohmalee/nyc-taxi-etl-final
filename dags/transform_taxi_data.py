import pandas as pd
import logging

def transform_taxi_data(**context):
    # 1. Pull the clean CSV path from XCom
    clean_path = context["ti"].xcom_pull(key="clean_path", task_ids="clean_taxi_data")
    
    if not clean_path:
        raise ValueError("No clean_path found in XCom.")

    logging.info(f"Loading clean data from: {clean_path}")
    df = pd.read_csv(clean_path)

    # แปลงเวลาเป็น datetime นะ
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    # 2. Compute derived features
    logging.info("Computing derived features...")
    df["trip_duration_minutes"] = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60.0
    df["speed_mph"] = df["trip_distance"] / (df["trip_duration_minutes"] / 60.0)
    df["fare_per_mile"] = df["fare_amount"] / df["trip_distance"]
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_day_of_week"] = df["tpep_pickup_datetime"].dt.dayofweek
    df["is_weekend"] = df["pickup_day_of_week"] >= 5

    # 3. Filter out unrealistic trips
    initial_count = len(df)
    df = df[(df["speed_mph"] <= 80) & (df["trip_duration_minutes"] >= 1)]
    removed_count = initial_count - len(df)
    logging.info(f"Filtered out {removed_count} unrealistic trips (speed > 80 mph or duration < 1 min).")

    # 5. Log descriptive statistics
    logging.info("Descriptive statistics for derived features:")
    new_columns = ["trip_duration_minutes", "speed_mph", "fare_per_mile", "pickup_hour", "pickup_day_of_week"]
    logging.info("\n" + df[new_columns].describe().to_string())

    # 4. Save transformed data
    transformed_path = "/tmp/nyc_taxi_transformed.csv"
    df.to_csv(transformed_path, index=False)
    logging.info(f"Saved transformed data to: {transformed_path}")

    # Push to XCom
    context["ti"].xcom_push(key="transformed_path", value=transformed_path)

    return transformed_path