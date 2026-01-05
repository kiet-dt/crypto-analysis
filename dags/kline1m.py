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
    # {'stream': 'btcusdt@kline_1m', 'data': {'e': 'kline', 'E': 1764138982015, 's': 'BTCUSDT',
    #                                         'k': {'t': 1764138960000, 'T': 1764139019999, 's': 'BTCUSDT', 'i': '1m',
    #                                               'f': 5568561770, 'L': 5568562176, 'o': '87539.39000000',
    #                                               'c': '87528.00000000', 'h': '87539.40000000', 'l': '87528.00000000',
    #                                               'v': '1.68562000', 'n': 407, 'x': False, 'q': '147547.64726560',
    #                                               'V': '0.03807000', 'Q': '3332.41186790', 'B': '0'}}}
    data['symbol'] = res['data']['s']
    data['event_time'] = res['data']['E']
    data['start_time'] = res['data']['k']['t']
    data['close_time'] = res['data']['k']['T']
    data['open'] = float(res['data']['k']['o'])
    data['close'] = float(res['data']['k']['c'])
    data['high'] = float(res['data']['k']['h'])
    data['low'] = float(res['data']['k']['l'])
    data['volume'] = float(res['data']['k']['v'])
    data['num_trades'] = res['data']['k']['n']
    data['taker_buy_quote_volume'] = float(res['data']['k']['Q'])
    data['is_closed'] = res['data']['k']['x']

    return data

async def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    url = "wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1m/ethusdt@kline_1m/bnbusdt@kline_1m/solusdt@kline_1m/xrpusdt@kline_1m"

    try:
        async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
            print("Connected!")

            curr_time = time.time()
            while (time.time() - curr_time) < 600:
                try:
                    msg = await ws.recv()
                    res = json.loads(msg)
                    data = format_data(res)

                    producer.send('kline1m_automation', json.dumps(data).encode('utf-8'))
                except Exception as e:
                    logging.error(e)
                    continue
    finally:
        producer.flush()
        producer.close()

def run_stream_data():
    asyncio.run(stream_data())

with DAG(
    dag_id='kline1m_automation',
    default_args=default_args,
    schedule_interval='@daily',
    description='Daily kline1m automation',
    catchup=False
) as dag:
    streaming_task = PythonOperator(
        task_id='stream_kline1m_from_api',
        python_callable=run_stream_data,
    )

    streaming_task

