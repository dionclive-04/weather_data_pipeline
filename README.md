# 🌤️ Real-Time Weather & Air Quality Data Pipeline

**Real-time Data Engineering Project | Kafka | Apache Flink | Supabase | Grafana**

---

## 🚀 Project Overview

This project is a **real-time data pipeline** that fetches live weather and air quality data from **OpenWeather API**, processes it through **Apache Kafka** and **Apache Flink**, stores it in **Supabase (Postgres)**, and visualizes it in **Grafana**.

The pipeline is designed to handle **high-throughput data streams**, enabling near real-time insights into global weather conditions and air quality across major cities.

---

## 🛠️ How It Works

1. **Data Ingestion**

   * Fetches **3,000 messages per minute** from OpenWeather API:

     * **1,500 weather updates** from top cities
     * **1,500 Air Quality Index (AQI) updates** from 1,500 cities
   * Publishes data into a **Kafka producer**.

2. **Data Processing with Apache Flink**

   * Consumes data from the Kafka topic
   * Performs transformations and enrichment
   * Publishes processed data to another Kafka topic

3. **Data Storage**

   * Consumes the processed data
   * Writes it into **Supabase (PostgreSQL)** for persistent storage

4. **Visualization**

   * Grafana connects to Supabase to visualize the data
   * Real-time dashboards display weather and AQI metrics globally

---

## ⚡ Tech Stack

| Layer             | Technology            |
| ----------------- | --------------------- |
| Data Source       | OpenWeather API       |
| Messaging Queue   | Apache Kafka          |
| Stream Processing | Apache Flink          |
| Database          | Supabase (PostgreSQL) |
| Visualization     | Grafana               |
| Language          | Python                |

---

## 📊 Key Features

* Handles **high-frequency data streams** (3,000 messages/min)
* Real-time processing with **Apache Flink**
* Scalable **Kafka-based architecture**
* Persistent storage in **Supabase (Postgres)**
* Interactive **Grafana dashboards**
* Supports **weather and air quality monitoring** for 1,500 cities

---

## 💻 Getting Started

### Prerequisites

* Python 3.10+
* Apache Kafka
* Apache Flink
* Supabase account / Postgres
* Grafana

### Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/weather-data-pipeline.git
cd weather-data-pipeline
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Configure your **OpenWeather API key** in `.env`:

```env
OPENWEATHER_API_KEY=your_api_key
KAFKA_BROKER=localhost:9092
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
```

4. Start Kafka, Flink, and Supabase
5. Run the Python scripts to start the pipeline

---

## 📈 Visualization

* Connect **Grafana** to your Supabase database
* Use pre-built dashboards to monitor **real-time weather and AQI data**

---

## ⭐ Why This Project?

* **Real-time analytics** for weather and air quality
* Demonstrates **end-to-end data engineering workflow**
* Built using industry-standard **streaming and messaging tools**
* Highly scalable and modular

---

## 📂 Repository Structure

```
weather-data-pipeline/
│
├── data_ingestion/         # Kafka producer scripts
├── flink_processing/       # Flink consumer/producer jobs
├── database/               # Scripts for Supabase/Postgres integration
├── visualization/          # Grafana dashboards
├── requirements.txt
├── README.md
└── .env.example
```

---

## 🔗 Links

* OpenWeather API: [https://openweathermap.org/api](https://openweathermap.org/api)
* Supabase: [https://supabase.com](https://supabase.com)
* Grafana: [https://grafana.com](https://grafana.com)
* Apache Kafka: [https://kafka.apache.org](https://kafka.apache.org)
* Apache Flink: [https://flink.apache.org](https://flink.apache.org)

---

## 📬 Contact

Dion Clive Saldanha

* GitHub: [https://github.com/yourusername](https://github.com/yourusername)
* LinkedIn: [https://linkedin.com/in/yourprofile](https://linkedin.com/in/yourprofile)

---

If you want, I can also **make a version with GIFs/screenshots of Grafana dashboards and pipeline flow**, which will **increase attention and engagement** drastically on GitHub.

Do you want me to do that next?
