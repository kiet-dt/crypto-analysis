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
    # {'stream': 'bnbusdt@trade',
    #  'data': {'e': 'trade', 'E': 1764136641261, 's': 'BNBUSDT', 't': 1329940898, 'p': '861.46000000', 'q': '0.00100000',
    #           'T': 1764136641261, 'm': True, 'M': True}}

    data['symbol'] = res['data']['s']
    data['price'] = float(res['data']['p'])
    data['quantity'] = float(res['data']['q'])
    data['trade_time'] = res['data']['T']
    data['buyer_is_maker'] = res['data']['m']

    return data


async def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    url = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/bnbusdt@trade/solusdt@trade/xrpusdt@trade"

    try:
        async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
            print("Connected!")

            curr_time = time.time()
            while (time.time() - curr_time) < 600:
                try:
                    msg = await ws.recv()
                    res = json.loads(msg)
                    data = format_data(res)

                    producer.send('trade_automation', json.dumps(data).encode('utf-8'))
                except Exception as e:
                    logging.error(e)
                    continue
    finally:
        producer.flush()
        producer.close()


def run_stream_data():
    asyncio.run(stream_data())


with DAG(
        dag_id='trade_automation',
        default_args=default_args,
        schedule_interval='@daily',
        description='Daily trade automation',
        catchup=False
) as dag:
    streaming_task = PythonOperator(
        task_id='stream_trade_from_api',
        python_callable=run_stream_data,
    )

    streaming_task

