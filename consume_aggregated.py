import json
from kafka import KafkaConsumer
from datetime import datetime, timedelta
from supabase import create_client  # Uncomment when using Supabase
from airflow import DAG
from airflow.operators.python import PythonOperator

# -------------------------------
# AQI Calculation
# -------------------------------
def calc_sub_index(C, breakpoints):
    for Clow, Chigh, Ilow, Ihigh in breakpoints:
        if Clow <= C <= Chigh:
            return ((Ihigh - Ilow) / (Chigh - Clow)) * (C - Clow) + Ilow
    return None

pm25_breakpoints = [(0.0, 12.0, 0, 50), (12.1, 35.4, 51, 100), (35.5, 55.4, 101, 150),
                    (55.5, 150.4, 151, 200), (150.5, 250.4, 201, 300), (250.5, 500.4, 301, 500)]
pm10_breakpoints = [(0, 54, 0, 50), (55, 154, 51, 100), (155, 254, 101, 150),
                    (255, 354, 151, 200), (355, 424, 201, 300), (425, 604, 301, 500)]
no2_breakpoints = [(0, 53, 0, 50), (54, 100, 51, 100), (101, 360, 101, 150),
                   (361, 649, 151, 200), (650, 1249, 201, 300), (1250, 2049, 301, 500)]
o3_breakpoints = [(0, 54, 0, 50), (55, 70, 51, 100), (71, 85, 101, 150),
                  (86, 105, 151, 200), (106, 200, 201, 300)]
def convert_to_ppb(ugm3, mw):
    if ugm3 is None:
        return None
    return (ugm3 * 24.45) / mw

def calculate_aqi(pm25, pm10, no2, o3):
    sub_indices = []
    if pm25 is not None:
        sub_indices.append(calc_sub_index(pm25, pm25_breakpoints))
    if pm10 is not None:
        sub_indices.append(calc_sub_index(pm10, pm10_breakpoints))
    if no2 is not None:
        no2_ppb = convert_to_ppb(no2, 46)
        sub_indices.append(calc_sub_index(no2_ppb, no2_breakpoints))
    if o3 is not None:
        o3_ppb = convert_to_ppb(o3, 48)
        sub_indices.append(calc_sub_index(o3_ppb, o3_breakpoints))
    return max(filter(None, sub_indices)) if sub_indices else None
# -------------------------------
# Main Functions
# -------------------------------
WEATHER_BATCH_SIZE = 50
AQI_BATCH_SIZE = 50
SUPABASE_URL = 'https://pxutjkudmwsrgeqmsxri.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB4dXRqa3VkbXdzcmdlcW1zeHJpIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NjEzMTM5MSwiZXhwIjoyMDcxNzA3MzkxfQ.hT2yuOnCoLuA9xD89TlQOK-8vB8tSnC67EXMndB3Mvw'
def consume_weather_messages():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    weather_consumer = KafkaConsumer(
        'aggregated-weather-topics',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id='weather_aggregated_group_test',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )
    batch_weather_data = []
    weather_msgs = weather_consumer.poll(1.0)
    for _, weather_list in weather_msgs.items():
        for weather_msg in weather_list:
            weather = weather_msg.value
            weather_data = {
                "country": weather.get("country"),
                "state": None,
                "city": weather.get("city"),
                "neighborhood": weather.get('neighborhood'),
                "temperature": weather.get('temperature'),
                "feels_like_temp": weather.get('feels_like_temp'),
                "wind_speed": weather.get('wind_speed'),
                "humidity": weather.get('humidity'),
                "pressure": weather.get('pressure'),
                "cloud_cover": weather.get('cloud_cover'),
                "visibility": weather.get('visibility'),
                "lat": weather['lat'],
                "lon": weather['lon'],
                "created_at": weather['created_at']
            }
            batch_weather_data.append(weather_data)
            while len(batch_weather_data) >= WEATHER_BATCH_SIZE:
                batch = batch_weather_data[:WEATHER_BATCH_SIZE]
                supabase.table('weather_aqi_data').insert(batch).execute()
                batch_weather_data = batch_weather_data[WEATHER_BATCH_SIZE:]
    print('Weather batch processed.')

def consume_aqi_messages():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    aqi_consumer = KafkaConsumer(
        'aggregated_aqi_producers',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id='aqi_group_test',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )
    batch_aqi_data = []
    aqi_msgs = aqi_consumer.poll(1.0)
    for _, aqi_list in aqi_msgs.items():
        for aqi_msg in aqi_list:
            aqi = aqi_msg.value
            aqi_data = {
                "co": aqi.get('co', "Unknown"),
                "no": aqi.get('no', "Unknown"),
                "no2": aqi.get('no2', "Unknown"),
                "o3": aqi.get('o3', "Unknown"),
                "so2": aqi.get('so2', "Unknown"),
                "pm2_5": aqi.get('pm2_5', "Unknown"),
                "pm10": aqi.get('pm10', "Unknown"),
                "nh3": aqi.get('nh3', "Unknown"),
                "aqi": calculate_aqi(aqi.get('pm2_5', 0), aqi.get('pm10', 0), aqi.get('no2', 0), aqi.get('o3', 0)),
                "lat": aqi.get('lat', "Unknown"),
                "lon": aqi.get('lon', "Unknown"),
                "created_at": aqi.get('created_at', "Unknown"),
                "city": aqi.get('city', "Unknown"),
                "country": aqi.get('country', "Unknown"),
            }
            batch_aqi_data.append(aqi_data)
            while len(batch_aqi_data) >= AQI_BATCH_SIZE:
                batch = batch_aqi_data[:AQI_BATCH_SIZE]
                supabase.table('aqi_data').insert(batch).execute()
                batch_aqi_data = batch_aqi_data[AQI_BATCH_SIZE:]
    print('AQI batch processed.')

# -------------------------------
# DAG Definition
# -------------------------------
default_args = {
    'owner': 'dion',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='consumer_aggregated_dag',
    default_args=default_args,
    description='Consume weather & AQI data and insert into Supabase',
    schedule=timedelta(minutes=5),  # Airflow 2.7+ preferred parameter
    start_date=datetime(2025, 9, 24),
    catchup=False
) as dag:

    consume_weather_task = PythonOperator(
        task_id='consume_weather_messages',
        python_callable=consume_weather_messages
    )

    consume_aqi_task = PythonOperator(
        task_id='consume_aqi_messages',
        python_callable=consume_aqi_messages
    )

    consume_weather_task >> consume_aqi_task
