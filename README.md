# Weather & Air Quality Streaming Pipeline

A Python data engineering project exploring weather and air quality ingestion, Kafka messaging, Flink windowed aggregation, and storage in Supabase.

## Architecture

```text
OpenWeather API
    -> Python producer
    -> Kafka: weather-topic / aqi_topic_producer
    -> Flink: two-minute processing-time windows, grouped by city
    -> Kafka: aggregated-weather-topic / aggregated_aqi_producer
    -> Airflow consumer tasks
    -> Supabase (PostgreSQL)
    -> Grafana (external dashboard setup)
```

## Repository guide

| File | Responsibility |
| --- | --- |
| [producer_weather.py](producer_weather.py) | Fetch weather and air pollution data asynchronously and publish JSON to Kafka |
| [aggreagate_incoming_data.py](aggreagate_incoming_data.py) | Parse records, convert weather temperatures to Celsius, aggregate by city, and publish results |
| [consume_aggregated.py](consume_aggregated.py) | Define an Airflow DAG with consumer tasks that batch inserts into Supabase |
| [requirements.txt](requirements.txt) | Python dependencies |

The aggregation filename above matches the existing repository spelling.

## Setup status

This repository contains the pipeline scripts. It is a learning project that requires configuration and integration fixes before an end-to-end run; it is not a one-command deployment.

```bash
git clone https://github.com/dionclive-04/weather_data_pipeline.git
cd weather_data_pipeline
pip install -r requirements.txt
```

Use an isolated Python environment. You will also need Kafka, a compatible Flink/PyFlink and Java installation, the Kafka connector JARs, Airflow, an OpenWeather account, and Supabase.

Before running:

1. Supply a city CSV containing `name`, `country_name`, `latitude`, and `longitude`. The producer currently references a machine-specific path; the CSV is not included.
2. Configure the producer's OpenWeather credential and the consumer's Supabase credentials through environment variables or a secret manager before deployment. The current scripts contain placeholders; they do not load a `.env` file.
3. Update the absolute connector JAR paths in the Flink job for your installation.
4. Align the consumer topic names with the Flink output topics. The current consumer uses `aggregated-weather-topics` and `aggregated_aqi_producers`, while Flink emits the singular names shown in the diagram.
5. Fix the missing comma following the weather consumer's `bootstrap_servers` argument in `consume_aggregated.py`.
6. Create the Supabase tables expected by the consumer: `weather_aqi_data` and `aqi_data`. A schema migration is not included.

After resolving those requirements, run the producer and Flink job as separate processes and load the consumer DAG into Airflow. The DAG is configured to run every five minutes. Grafana dashboards must be configured separately; dashboard exports are not included.

## Engineering notes

- Aggregations use processing time and two-minute windows.
- The producer fetches weather and AQI concurrently in batches of 50 cities.
- Actual throughput depends on the city list, API limits, latency, and infrastructure. This repository does not include a throughput benchmark.
- The consumer batches inserts in groups of 50. Handling incomplete batches, retries, and replay without duplicate writes needs further work.

## Next milestones

- Move credentials, paths, broker addresses, and topic names into validated configuration.
- Add fixtures and tests for parsing, aggregation, and partial API failures.
- Include database schema, reproducible service setup, and dashboard exports.
- Measure end-to-end latency, message loss, and replay behavior.

## Author

[Dion Clive Saldanha](https://github.com/dionclive-04) · [LinkedIn](https://www.linkedin.com/in/dion-saldanha-97b2aa243/)
