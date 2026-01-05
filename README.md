# Crypto Analysis Pipeline
A real-time cryptocurrency data analysis pipeline that collects, processes, and visualizes market data from Binance exchange using Apache Airflow, Kafka, Spark Streaming, and Elasticsearch.

## Overview
This project implements an end-to-end data pipeline for cryptocurrency market analysis. It streams real-time data from Binance WebSocket API, processes it through Kafka and Spark, stores it in Elasticsearch, and provides visualization dashboards.

## Architecture
Binance WebSocket API
↓
Apache Airflow (DAGs)
↓
Kafka Topics
↓
Spark Streaming
↓
Elasticsearch
↓
Visualization (Grafana)

## Data Sources
The pipeline collects the following types of market data for multiple cryptocurrency pairs:
- **Ticker**: 24-hour ticker price statistics
- **Trade**: Real-time trade data
- **Depth**: Order book depth updates
- **BookTicker**: Best bid/ask prices
- **Kline1m**: 1-minute candlestick data

### Supported Trading Pairs
- BTCUSDT (Bitcoin)
- ETHUSDT (Ethereum)
- BNBUSDT (Binance Coin)
- SOLUSDT (Solana)
- XRPUSDT (Ripple)

## Project Structure
crypto-analysis/
├── dags/ # Airflow DAG definitions
│ ├── bookTicker.py # BookTicker data streaming
│ ├── ticker.py # Ticker data streaming
│ ├── trade.py # Trade data streaming
│ ├── depth.py # Depth data streaming
│ └── kline1m.py # Kline 1-minute data streaming
├── schemas/ # Spark data schemas
│ ├── bookTicker_schema.py
│ ├── ticker_schema.py
│ ├── trade_schema.py
│ ├── depth_schema.py
│ └── kline1m_schema.py
├── spark_stream.py # Spark Streaming application
├── jars/ # Required JAR dependencies
├── dashboard/ # Generated visualization dashboards
│ ├── ticker-24h/
│ ├── depth/
│ ├── kline-1m/
│ └── trade-flow/
├── docker-compose.yaml # Docker Compose configuration
├── Dockerfile.airflow # Airflow Docker image
├── Dockerfile.spark # Spark Docker image
└── requirements.txt # Python dependencies


## Getting Started
### Prerequisites
- Docker Desktop (or Docker Engine + Docker Compose)
- At least 8GB RAM available
- Ports available: 8080, 9092, 9200, 5601, 3000, 9021, 7077, 9090
### Installation
1. **Clone the repository**ash
   git clone <repository-url>
   cd crypto-analysis
   2. **Build Docker images**
   docker-compose build
   3. **Start all services**
   docker-compose up -d
   4. **Wait for services to initialize**
   The first startup may take a few minutes. Check service health:ash
   docker-compose ps
   ### Accessing Services
Once all services are running, you can access:
- **Airflow Web UI**: http://localhost:8080
  - Username: `admin`
  - Password: `admin`
- **Kafka Control Center**: http://localhost:9021
- **Elasticsearch**: http://localhost:9200
- **Kibana**: http://localhost:5601
- **Grafana**: http://localhost:3000
  - Username: `admin`
  - Password: `admin`
- **Spark Master UI**: http://localhost:9090
## 📝 Configuration
### Airflow DAGs
Each DAG is configured to run daily and streams data for 10 minutes (600 seconds). You can modify the streaming duration in the respective DAG files:
- `dags/bookTicker.py`
- `dags/ticker.py`
- `dags/trade.py`
- `dags/depth.py`
- `dags/kline1m.py`
### Kafka Topics
The following topics are automatically created:
- `ticker_automation`
- `trade_automation`
- `depth_automation`
- `bookTicker_automation`
- `kline1m_automation`
### Elasticsearch Indices
Data is stored in the following indices:
- `ticker_index`
- `trade_index`
- `depth_index`
- `bookticker_index`
- `kline1m_index`
## Running the Pipeline
### Manual Execution
1. **Start Spark Streaming job** (if not running automatically):
   docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.4.2.jar,/opt/spark/jars/kafka-clients-3.4.0.jar,/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.4.2.jar,/opt/spark/jars/commons-pool2-2.11.1.jar,/opt/spark/jars/elasticsearch-spark-30_2.12-8.11.3.jar /opt/spark-jobs/spark_stream.py
   2. **Trigger Airflow DAGs**:
   - Navigate to Airflow UI: http://localhost:8080
   - Enable and trigger the desired DAGs:
     - `bookTicker_automation`
     - `ticker_automation`
     - `trade_automation`
     - `depth_automation`
     - `kline1m_automation`
### Scheduled Execution

DAGs are configured to run daily at midnight (00:00 UTC). They will automatically:
1. Connect to Binance WebSocket API
2. Stream data for 10 minutes
3. Send data to Kafka topics
4. Spark processes and stores data in Elasticsearch

## Data Processing

### Spark Transformations

The Spark Streaming job performs the following transformations:

**Ticker Data**:
- Calculates spread (best_ask_price - best_bid_price)
- Converts event_time to timestamp format

**Trade Data**:
- Calculates trade_value (price × quantity)
- Converts trade_time to timestamp format

**Kline1m Data**:
- Calculates price_change_percent
- Calculates volatility (high - low)
- Converts timestamps for event_time, start_time, close_time

## Monitoring & Visualization

### Viewing Data in Kibana

1. Access Kibana: http://localhost:5601
2. Navigate to "Discover"
3. Select an index (e.g., `ticker_index`)
4. Explore and visualize your data

### Viewing Data in Grafana

1. Access Grafana: http://localhost:3000
2. Configure Elasticsearch as a data source
3. Create dashboards for real-time monitoring

### Generated Dashboards

Pre-generated dashboard images are stored in the `dashboard/` directory:
- `ticker-24h/`: 24-hour ticker overview
- `depth/`: Order book depth visualizations
- `kline-1m/`: 1-minute candlestick charts
- `trade-flow/`: Trade flow visualizations

## Development

### Adding New Trading Pairs

1. Update the WebSocket URL in the respective DAG file:
   url = "wss://stream.binance.com:9443/stream?streams=btcusdt@ticker/.../newpair@ticker"
   2. Restart the DAG to apply changes

### Modifying Data Schemas

1. Update the schema in `schemas/<schema_name>_schema.py`
2. Update the Spark transformation logic in `spark_stream.py`
3. Restart Spark Streaming job

## Dependencies

### Python Packages
- `kafka-python==2.2.15`
- `elasticsearch==8.12.0`
- `websockets==13.1`

### JAR Files
- `spark-sql-kafka-0-10_2.12-3.4.2.jar`
- `kafka-clients-3.4.0.jar`
- `elasticsearch-spark-30_2.12-8.11.3.jar`
- `commons-pool2-2.11.1.jar`
- `spark-token-provider-kafka-0-10_2.12-3.4.2.jar`

