import asyncio
import json
import websockets
import logging
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

default_args = {
    'owner': 'tuankiet',
    'start_date': datetime(2025, 11, 20),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def format_data(res):
    data={}
    # {
    # 'e': '24hrTicker', 'E': 1764089766028, 's': 'BTCUSDT', 'p': '-227.56000000', 'P': '-0.261', 'w': '87734.25180474', 'x': '87239.99000000', 'c': '87012.44000000',
    # 'Q': '0.00311000', 'b': '87012.43000000', 'B': '0.89795000', 'a': '87012.44000000', 'A': '10.13511000', 'o': '87240.00000000', 'h': '89228.00000000',
    # 'l': '86116.00000000', 'v': '21912.95119000', 'q': '1922516377.48847200', 'O': 1764003366012, 'C': 1764089766012, 'F': 5561141720, 'L': 5566219247, 'n': 5077528
    # }
    data['symbol'] = res['data']['s']
    data['open'] = float(res['data']['o'])
    data['close'] = float(res['data']['c'])
    data['high'] = float(res['data']['h'])
    data['low'] = float(res['data']['l'])
    data['price_change_percent'] = float(res['data']['P'])
    data['volume'] = float(res['data']['v'])
    data["quote_volume"] = float(res['data']['q'])
    data["best_bid_price"] = float(res['data']['b'])
    data["best_ask_price"] = float(res['data']['a'])
    data['num_trades'] = res['data']['n']
    data["event_time"] = res['data']['E']

    return data

async def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    url = "wss://stream.binance.com:9443/stream?streams=btcusdt@ticker/ethusdt@ticker/bnbusdt@ticker/solusdt@ticker/xrpusdt@ticker"

    try:
        async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
            print("Connected!")

            curr_time = time.time()
            while (time.time() - curr_time) < 600:
                try:
                    msg = await ws.recv()
                    res = json.loads(msg)
                    data = format_data(res)

                    producer.send('ticker_automation', json.dumps(data).encode('utf-8'))
                except Exception as e:
                    logging.error(e)
                    continue
    finally:
        producer.flush()
        producer.close()

def run_stream_data():
    asyncio.run(stream_data())

with DAG(
    dag_id='ticker_automation',
    default_args=default_args,
    schedule_interval='@daily',
    description='Daily ticker automation',
    catchup=False
) as dag:
    streaming_task = PythonOperator(
        task_id='stream_ticker_from_api',
        python_callable=run_stream_data,
    )

    streaming_task
