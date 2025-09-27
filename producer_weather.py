import requests
from datetime import datetime, timedelta, timezone
import certifi
from kafka import KafkaProducer
import json
import time
import pandas as pd
import asyncio
import aiohttp
import os

class WeatherProducer:
    def __init__(self):
        self.df = pd.read_csv(r'/home/dion/airflow_project/app/top_1000_cities.csv')
        self.open_weather_api = '75d0e4309d0b73172a1e9fe1c73f2240'
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    async def fetch_weather(self, session, lat, lon):
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={self.open_weather_api}"
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                print("Error: loading the data")

    async def fetch_aqi(self, session, lat, lon):
        url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={self.open_weather_api}"
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                print("Error: loading the data")

    async def process_in_batch(self, session, batch):
        weather_tasks = [self.fetch_weather(session, data.latitude, data.longitude) for data in batch]
        aqi_tasks = [self.fetch_aqi(session, data.latitude, data.longitude) for data in batch]

        weather_results, aqi_results = await asyncio.gather(asyncio.gather(*weather_tasks), asyncio.gather(*aqi_tasks))
        for city, weather_json, aqi_json in zip(batch, weather_results, aqi_results):
            if weather_json:
                timezone_offset = weather_json["timezone"]
                utc_time = datetime.fromtimestamp(weather_json['dt'], tz=timezone.utc)
                local_time = utc_time + timedelta(seconds=timezone_offset)

                weather_json['created_at'] = local_time
                aqi_json['created_at'] = local_time
                weather_json['city'] = city.name
                weather_json['country'] = city.country_name
                aqi_json['country'], aqi_json['city'] = city.country_name, city.name
                if ('created_at' in weather_json  and 'created_at' in aqi_json) and isinstance(weather_json['created_at'], datetime):
                    weather_json['created_at'] = weather_json['created_at'].strftime('%Y-%m-%d %H:%M:%S')
                    aqi_json['created_at'] = aqi_json['created_at'].strftime('%Y-%m-%d %H:%M:%S')
                self.producer.send('weather-topic', key=str(city.name).encode('utf-8'), value=weather_json)
                self.producer.send('aqi_topic_producer', key=str(city.name).encode('utf-8'), value=aqi_json)
        self.producer.flush()

    async def main(self):
        while True:
            async with aiohttp.ClientSession() as session:
                for i in range(0, len(self.df), 50):
                    batch = list(self.df.iloc[i:i + 50].itertuples())
                    await self.process_in_batch(session, batch)
                    await asyncio.sleep(1)
            print('No Data')
            await asyncio.sleep(60)


producers = WeatherProducer()
asyncio.run(producers.main())
