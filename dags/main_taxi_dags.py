from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# ==========================================
# 1. Import ฟังก์ชันจากไฟล์ของเพื่อนๆ แต่ละคน
# (สมมติว่าไฟล์ทั้งหมดอยู่ในโฟลเดอร์ dags/ เดียวกัน)
# ==========================================
from ingest_taxi_data import ingest_taxi_data
from clean_taxi_data import clean_taxi_data
from transform_taxi_data import transform_taxi_data # แก้ชื่อไฟล์ให้สะกดถูกด้วยนะครับ
from load_taxi_model import load_taxi_model

# ==========================================
# 2. ตั้งค่า DAG
# ==========================================
default_args = {
    'owner': 'team_project', 
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='nyc_taxi_master_pipeline', 
    default_args=default_args,
    description='ETL pipeline importing from separate files',
    schedule='@daily', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['taxi', 'group_project'],
) as dag:

    # ==========================================
    # 3. สร้าง Operator โดยเรียกใช้ฟังก์ชันที่ Import มา
    # ==========================================
    task_ingest = PythonOperator(
        task_id='ingest_taxi_data', 
        python_callable=ingest_taxi_data, # เรียกใช้ฟังก์ชันจากไฟล์เพื่อนคนที่ 1
    )

    task_clean = PythonOperator(
        task_id='clean_taxi_data',
        python_callable=clean_taxi_data,  # เรียกใช้ฟังก์ชันจากไฟล์เพื่อนคนที่ 2
    )

    task_transform = PythonOperator(
        task_id='transform_taxi_data',
        python_callable=transform_taxi_data, # เรียกใช้ฟังก์ชันจากไฟล์เพื่อนคนที่ 3
    )

    task_load = PythonOperator(
        task_id='load_taxi_model',
        python_callable=load_taxi_model, # เรียกใช้ฟังก์ชันจากไฟล์เพื่อนคนที่ 4
    )

    # ==========================================
    # 4. กำหนดลำดับการทำงาน
    # ==========================================
    task_ingest >> task_clean >> task_transform >> task_load