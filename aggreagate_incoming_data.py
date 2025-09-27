from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import AggregateFunction
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.typeinfo import Types, RowTypeInfo
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, Schema, DataTypes
from pyflink.common import Row
import json
from datetime import datetime, timedelta

# ----------------------------
# 1. Initialize Flink environment
# ----------------------------
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# ----------------------------
# 2. Add required JAR files
# ----------------------------
env.add_jars(
    "file:///home/dion/flink/flink-connector-kafka-3.4.0-1.20.jar",
    "file:///home/dion/flink/kafka-clients-3.4.0.jar",
    "file:///home/dion/flink/postgresql-42.6.0.jar",
    "file:///home/dion/flink/flink-connector-jdbc-postgres-3.3.0.jar",
    "file:///home/dion/flink/flink-connector-jdbc-core-3.3.0-1.20.jar",
)

# ----------------------------
# 3. Kafka consumer configuration
# ----------------------------
kafka_weather_props = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "flink-weather-consumer",
    "auto.offset.reset": "earliest"
}

kafka_aqi_props = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "flink-aqi-consumer",
    "auto.offset.reset": "earliest"
}


weather_consumer = FlinkKafkaConsumer(
    topics="weather-topic",
    deserialization_schema=SimpleStringSchema(),
    properties=kafka_weather_props
)

aqi_consumer = FlinkKafkaConsumer(
    topics="aqi_topic_producer",
    deserialization_schema=SimpleStringSchema(),
    properties=kafka_aqi_props
)



# ----------------------------
# 4. Define TypeInformation with named fields
# ----------------------------
weather_row_type_info = RowTypeInfo(
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.FLOAT(), Types.FLOAT(), Types.FLOAT(),
     Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.DOUBLE(), Types.DOUBLE(), Types.SQL_TIMESTAMP()],
    ["country", "state", "city", "neighbourhood",
     "temperature", "feels_like_temp", "wind_speed",
     "humidity", "pressure", "cloud_cover", "visibility", "lat", "lon", "created_at"]
)

aqi_row_type_info = RowTypeInfo(
    [Types.FLOAT(),Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(),
     Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.SQL_TIMESTAMP(), Types.STRING(), Types.STRING()],
    ["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3", "aqi", "lat", "lon", "created_at", "city", "country"]
)
# ----------------------------
# 5. Parse JSON from Kafka and return a Row
# ----------------------------
def parse_weather_record(raw_json):
    try:
        record = json.loads(raw_json)
        country = record.get('country', 'Unknown')
        state = record.get('state', 'Unknown')
        city = record.get('city', record.get('name', 'Unknown'))
        neighbourhood = record.get('neighbourhood', 'Unknown')
        main_data = record.get('main', {})
        wind_data = record.get('wind', {})
        clouds_data = record.get('clouds', {})
        temperature = round(main_data.get('temp', 273.15) - 273.15, 2)
        feels_like_temp = round(main_data.get('feels_like', 273.15) - 273.15, 2)
        wind_speed = round(wind_data.get('speed', 0.0), 2)
        humidity = int(main_data.get('humidity', 0))
        pressure = int(main_data.get('pressure', 0))
        cloud_cover = int(clouds_data.get('all', 0))
        visibility = int(record.get('visibility', 0))
        lat = record.get('coord', {}).get('lat', 0)
        lon = record.get('coord', {}).get('lon', 0)
        created_at_str = record.get('created_at')
        if created_at_str:
            try:
                # Convert back to datetime for Flink SQL_TIMESTAMP
                created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S")
            except Exception:
                created_at = None
        else:
            created_at = None

        return Row(
            country, state, city, neighbourhood,
            temperature, feels_like_temp, wind_speed,
            humidity, pressure, cloud_cover, visibility, lat, lon, created_at
        )

    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
        print(f"Parse error: {e}, raw_data: {raw_json[:100]}...")
        return Row("Unknown", "Unknown", "Unknown", "Unknown", 0.0, 0.0, 0.0, 0, 0, 0, 0, 0.0, 0.0, None)

def parse_aqi_record(raw_json):
    try:
        record = json.loads(raw_json)
        co = record.get('list', [{}])[0].get('components', {}).get('co', {})
        no = record.get('list', [{}])[0].get('components', {}).get('no', {})
        no2 = record.get('list', [{}])[0].get('components', {}).get('no2', {})
        o3 = record.get('list', [{}])[0].get('components', {}).get('o3', {})
        so2 = record.get('list', [{}])[0].get('components', {}).get('so2', {})
        pm2_5 = record.get('list', [{}])[0].get('components', {}).get('pm2_5', {})
        pm10 = record.get('list', [{}])[0].get('components', {}).get('pm10', {})
        nh3 = record.get('list', [{}])[0].get('components', {}).get('nh3', {})
        aqi = record.get('list', [{}])[0].get('main', {}).get('aqi', {})
        lat = record.get('coord', {}).get('lat', 0)
        lon = record.get('coord', {}).get('lon', 0)
        created_at_str = record.get('created_at')
        city = record.get('city', 'Unknown')
        country = record.get('country', 'Unknown')
        if created_at_str:
            try:
                # Convert back to datetime for Flink SQL_TIMESTAMP
                created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S")
            except Exception:
                created_at = None
        else:
            created_at = None

        return Row(
            co, no, no2, o3,
            so2, pm2_5, pm10,
            nh3, aqi, lat, lon, created_at, city, country
        )

    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
        print(f"Parse error: {e}, raw_data: {raw_json[:100]}...")
        return Row(0.0, 0.0, 0.0 , 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, None, None, None)

# ----------------------------
# 6. Aggregate function
# ----------------------------
class WeatherAggregateFunction(AggregateFunction):
    def create_accumulator(self):
        return (0.0, 0.0, 0.0, 0, 0, 0, 0, 0, None, 0.0, 0.0, None)

    def add(self, value, accumulator):
        key_info = accumulator[8] or (value[0], value[1], value[2], value[3], value[11], value[12])
        created_at = value[13] or accumulator[11]
        return (
            accumulator[0] + value[4],
            accumulator[1] + value[5],
            accumulator[2] + value[6],
            accumulator[3] + value[7],
            accumulator[4] + value[8],
            accumulator[5] + value[9],
            accumulator[6] + value[10],
            accumulator[7] + 1,
            key_info,
            key_info[4],
            key_info[5],
            created_at
        )

    def get_result(self, accumulator):
        count = accumulator[7]
        if count == 0:
            return None
        country, state, city, neighbourhood, lat, lon = accumulator[8]
        return Row(
            country,
            state,
            city,
            neighbourhood,
            round(accumulator[0] / count, 2),
            round(accumulator[1] / count, 2),
            round(accumulator[2] / count, 2),
            int(accumulator[3] / count),
            int(accumulator[4] / count),
            int(accumulator[5] / count),
            int(accumulator[6] / count),
            lat,
            lon,
            accumulator[11]
        )

    def merge(self, acc1, acc2):
        return (
            acc1[0] + acc2[0],
            acc1[1] + acc2[1],
            acc1[2] + acc2[2],
            acc1[3] + acc2[3],
            acc1[4] + acc2[4],
            acc1[5] + acc2[5],
            acc1[6] + acc2[6],
            acc1[7] + acc2[7],
            acc1[8] or acc2[8],
            acc1[9] or acc2[9],
            acc1[10] or acc2[10],
            acc1[11] or acc2[11],
        )

class AqiAggregateFunction(AggregateFunction):
    def create_accumulator(self):
        # sums for 9 pollutants + count + metadata
        return (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0,
                None, None, None, None, None)
        # (co, no, no2, o3, so2, pm2_5, pm10, nh3, aqi, count, city, country, lat, lon, created_at)

    def add(self, value, acc):
        return (
            acc[0] + (value[0] or 0.0),   # co
            acc[1] + (value[1] or 0.0),   # no
            acc[2] + (value[2] or 0.0),   # no2
            acc[3] + (value[3] or 0.0),   # o3
            acc[4] + (value[4] or 0.0),   # so2
            acc[5] + (value[5] or 0.0),   # pm2_5
            acc[6] + (value[6] or 0.0),   # pm10
            acc[7] + (value[7] or 0.0),   # nh3
            acc[8] + (value[8] or 0.0),   # aqi
            acc[9] + 1,                   # count
            value[12] or acc[10],         # city
            value[13] or acc[11],         # country
            value[9]  or acc[12],         # lat
            value[10] or acc[13],         # lon
            value[11] or acc[14],         # created_at
        )

    def get_result(self, acc):
        count = acc[9]
        if count == 0:
            return None
        return Row(
            round(acc[0]/count, 2),  # co
            round(acc[1]/count, 2),  # no
            round(acc[2]/count, 2),  # no2
            round(acc[3]/count, 2),  # o3
            round(acc[4]/count, 2),  # so2
            round(acc[5]/count, 2),  # pm2_5
            round(acc[6]/count, 2),  # pm10
            round(acc[7]/count, 2),  # nh3
            round(acc[8]/count, 2),  # aqi
            acc[12],                 # lat
            acc[13],                 # lon
            acc[14],                 # created_at
            acc[10],                 # city
            acc[11],                 # country
        )

    def merge(self, acc1, acc2):
        return (
            acc1[0]+acc2[0], acc1[1]+acc2[1], acc1[2]+acc2[2],
            acc1[3]+acc2[3], acc1[4]+acc2[4], acc1[5]+acc2[5],
            acc1[6]+acc2[6], acc1[7]+acc2[7], acc1[8]+acc2[8],
            acc1[9]+acc2[9],
            acc1[10] or acc2[10],  # city
            acc1[11] or acc2[11],  # country
            acc1[12] or acc2[12],  # lat
            acc1[13] or acc2[13],  # lon
            acc1[14] or acc2[14],  # created_at
        )
# ----------------------------
# 7. Create streams
# ----------------------------
weather_stream = env.add_source(weather_consumer).map(parse_weather_record, output_type=weather_row_type_info)
weather_stream.print("Raw Weather Data")

aqi_stream = env.add_source(aqi_consumer).map(parse_aqi_record, output_type=aqi_row_type_info)
aqi_stream.print("Raw Aqi Data")

weather_aggregated_stream = weather_stream \
    .key_by(lambda record: record[2]) \
    .window(TumblingProcessingTimeWindows.of(Time.minutes(2))) \
    .aggregate(WeatherAggregateFunction(), output_type=weather_row_type_info)

aqi_aggregated_stream = aqi_stream \
    .key_by(lambda record: record[12]) \
    .window(TumblingProcessingTimeWindows.of(Time.minutes(2))) \
    .aggregate(AqiAggregateFunction(), output_type=aqi_row_type_info)

weather_aggregated_stream.print("Aggregated Weather Data")

# ----------------------------
# 8. Kafka sink configuration
# ----------------------------
producer_props = {
    "bootstrap.servers": "localhost:9092"
}

weather_aggregated_producer = FlinkKafkaProducer(
    topic="aggregated-weather-topic",
    serialization_schema=SimpleStringSchema(),
    producer_config=producer_props
)

aqi_aggregated_producer= FlinkKafkaProducer(
    topic="aggregated_aqi_producer",
    serialization_schema=SimpleStringSchema(),
    producer_config=producer_props
)

def weather_row_json(row):
    return json.dumps({
        "country": row[0],
        "state": row[1],
        "city": row[2],
        "neighbourhood": row[3],
        "temperature": row[4],
        "feels_like_temp": row[5],
        "wind_speed": row[6],
        "humidity": row[7],
        "pressure": row[8],
        "cloud_cover": row[9],
        "visibility": row[10],
        "lat": row[11],
        "lon": row[12],
        "created_at": row[13].strftime('%Y-%m-%d %H:%M:%S') if row[13] else None,
    })

def aqi_row_json(row):
    return json.dumps({
        "co": row[0],
        "no": row[1],
        "no2": row[2],
        "o3": row[3],
        "so2": row[4],
        "pm2_5": row[5],
        "pm10": row[6],
        "nh3": row[7],
        "aqi": row[8],
        "lat": row[9],
        "lon": row[10],
        "created_at": row[11].strftime('%Y-%m-%d %H:%M:%S') if row[11] else None,
        "city": row[12],
        "country": row[13],
    })
aggregated_weather_stream = weather_aggregated_stream.map(
    weather_row_json,
    output_type=Types.STRING()
).add_sink(weather_aggregated_producer)

aggregated_aqi_stream = aqi_aggregated_stream.map(
    aqi_row_json,
    output_type=Types.STRING()
).add_sink(aqi_aggregated_producer)

# aggregated_json_stream.add_sink(producer)
env.execute("Enhanced Weather Data Streaming Pipeline")
