def clean_taxi_data(**context):
    import pandas as pd
    import logging
# ingest logic (v2)
    # 1. ดึง Path ของไฟล์ดิบจาก XCom ที่ Task แรกส่งมา
    raw_path = context["ti"].xcom_pull(key="raw_path", task_ids="ingest_taxi_data")
    
    if not raw_path:
        raise ValueError("ไม่พบ raw_path ใน XCom กรุณาตรวจสอบให้แน่ใจว่า ingest_taxi_data ทำงานสำเร็จ")

    logging.info(f"Loading raw data from: {raw_path}")
    
    # 2. โหลดข้อมูลด้วย pandas
    df = pd.read_csv(raw_path, on_bad_lines='skip')
    initial_count = len(df)
    logging.info(f"Initial row count: {initial_count}")

    # 3. เริ่มกระบวนการทำความสะอาด และ 4. Log จำนวนบรรทัดที่ถูกลบในแต่ละขั้นตอน
    
    # ลบแถวที่มีค่า Null ในคอลัมน์สำคัญ
    df = df.dropna(subset=["fare_amount", "trip_distance", "tpep_pickup_datetime"])
    current_count = len(df)
    logging.info(f"Step 1: Removed {initial_count - current_count} rows with null values.")
    
    # แปลง tpep_pickup_datetime เป็น datetime
    prev_count = current_count
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
    df = df.dropna(subset=["tpep_pickup_datetime"])
    current_count = len(df)
    logging.info(f"Step 2: Removed {prev_count - current_count} rows due to invalid datetime parsing.")

    # ลบแถวที่ค่าโดยสาร <= 0 หรือ > 500
    prev_count = current_count
    df = df[(df["fare_amount"] > 0) & (df["fare_amount"] <= 500)]
    current_count = len(df)
    logging.info(f"Step 3: Removed {prev_count - current_count} rows due to invalid fare_amount.")

    # ลบแถวที่ระยะทาง <= 0 หรือ > 100 ไมล์
    prev_count = current_count
    df = df[(df["trip_distance"] > 0) & (df["trip_distance"] <= 100)]
    current_count = len(df)
    logging.info(f"Step 4: Removed {prev_count - current_count} rows due to invalid trip_distance.")

    # ลบแถวที่พิกัด GPS ผิดปกติ (ครอบคลุมทั้งกรณีที่มีคอลัมน์ pickup และ dropoff)
    prev_count = current_count
    if "pickup_latitude" in df.columns and "pickup_longitude" in df.columns:
        df = df[
            df["pickup_latitude"].between(40.4, 41.0) &
            df["pickup_longitude"].between(-74.3, -73.5)
        ]
    if "dropoff_latitude" in df.columns and "dropoff_longitude" in df.columns:
        df = df[
            df["dropoff_latitude"].between(40.4, 41.0) &
            df["dropoff_longitude"].between(-74.3, -73.5)
        ]
    current_count = len(df)
    logging.info(f"Step 5: Removed {prev_count - current_count} rows due to invalid coordinates.")

    logging.info(f"Cleaning complete. Remaining rows: {current_count}")

    # 5. บันทึกข้อมูลที่ทำความสะอาดแล้ว (ใช้ Path สำหรับรันเทสต์บนเครื่องของคุณ)
    clean_path = "/opt/airflow/dags/nyc_taxi_clean.csv"
    df.to_csv(clean_path, index=False)
    logging.info(f"Saved cleaned data to: {clean_path}")

    # 6. ส่ง Path ของไฟล์ที่สะอาดแล้วเข้า XCom สำหรับ Task ต่อไป
    context["ti"].xcom_push(key="clean_path", value=clean_path)

    return clean_path