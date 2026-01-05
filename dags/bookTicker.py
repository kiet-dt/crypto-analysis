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
#   "u": 81837520380,   // update id
#   "s": "BTCUSDT",     // symbol
#   "b": "86883.99000000", // best bid price
#   "B": "1.38027000",  // best bid quantity
#   "a": "86884.00000000", // best ask price
#   "A": "3.65088000"   // best ask quantity
# }
    data['update_id'] = res['data']['u']
    data['symbol'] = res['data']['s']
    data['best_bid_price'] = float(res['data']['b'])
    data['best_bid_qty'] = float(res['data']['B'])
    data['best_ask_price'] = float(res['data']['a'])
    data['best_ask_qty'] = float(res['data']['A'])

    return data

async def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    url = "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker/bnbusdt@bookTicker/solusdt@bookTicker/xrpusdt@bookTicker"

    try:
        async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
            print("Connected!")

            curr_time = time.time()
            while (time.time() - curr_time) < 600:
                try:
                    msg = await ws.recv()
                    res = json.loads(msg)
                    data = format_data(res)

                    producer.send('bookTicker_automation', json.dumps(data).encode('utf-8'))
                except Exception as e:
                    logging.error(e)
                    continue
    finally:
        producer.flush()
        producer.close()

def run_stream_data():
    asyncio.run(stream_data())

with DAG(
    dag_id='bookTicker_automation',
    default_args=default_args,
    schedule_interval='@daily',
    description='Daily bookTicker automation',
    catchup=False
) as dag:
    streaming_task = PythonOperator(
        task_id='stream_bookTicker_from_api',
        python_callable=run_stream_data,
    )

    streaming_task
