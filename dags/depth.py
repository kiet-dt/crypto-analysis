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
    # {'stream': 'solusdt@depth',
    #  'data': {'e': 'depthUpdate', 'E': 1764152475795, 's': 'SOLUSDT', 'U': 26475574123, 'u': 26475574382,
    #           'b': [['137.09000000', '619.10000000'], ['137.08000000', '26.74200000'], ['137.07000000', '119.09000000'], ['137.06000000', '197.79200000'], ['137.05000000', '388.93500000'], ['137.04000000', '471.96800000'], ['137.03000000', '466.44600000'], ['137.02000000', '441.64100000'], ['137.01000000', '751.40800000'], ['136.99000000', '199.30300000'], ['136.98000000', '248.42800000'], ['136.97000000', '430.33300000'], ['136.96000000', '452.66300000'], ['136.94000000', '208.96600000'], ['136.92000000', '343.25300000'], ['136.90000000', '649.25900000'], ['136.85000000', '303.29000000'], ['136.84000000', '747.58600000'], ['136.77000000', '1413.75600000'], ['136.74000000', '912.41100000'], ['136.73000000', '702.77700000'], ['136.70000000', '141.96200000'], ['136.67000000', '119.62600000'], ['136.66000000', '95.00900000'], ['136.62000000', '109.56000000'], ['136.33000000', '28.21400000'], ['136.24000000', '36.39600000'], ['135.98000000', '20.72900000']],
    #           'a': [['137.09000000', '0.00000000'], ['137.10000000', '35.71500000'], ['137.11000000', '106.28300000'], ['137.12000000', '18.40800000'], ['137.13000000', '63.76900000'], ['137.14000000', '179.78700000'], ['137.15000000', '267.28100000'], ['137.16000000', '301.66600000'], ['137.17000000', '380.02200000'], ['137.18000000', '258.53300000'], ['137.19000000', '574.79600000'], ['137.20000000', '953.35900000'], ['137.22000000', '267.40300000'], ['137.23000000', '296.08500000'], ['137.28000000', '226.48100000'], ['137.35000000', '113.64900000'], ['137.36000000', '163.39400000'], ['137.38000000', '204.71900000'], ['137.40000000', '347.90600000'], ['137.45000000', '172.21000000'], ['137.46000000', '283.55100000'], ['137.47000000', '110.95600000'], ['137.53000000', '126.42100000'], ['137.85000000', '301.75100000']]}}
    data['symbol'] = res['data']['s']
    data['event_time'] = res['data']['E']
    data['first_update_id'] = res['data']['U']
    data['last_update_id'] = res['data']['u']
    data['best_bid_price'] = float(res['data']['b'][0][0])
    data['best_bid_qty'] = float(res['data']['b'][0][1])
    data['best_ask_price'] = float(res['data']['a'][0][0])
    data['best_ask_qty'] = float(res['data']['a'][0][1])

    return data

async def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    url = "wss://stream.binance.com:9443/stream?streams=btcusdt@depth/ethusdt@depth/bnbusdt@depth/solusdt@depth/xrpusdt@depth"

    try:
        async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
            print("Connected!")

            curr_time = time.time()
            while (time.time() - curr_time) < 600:
                try:
                    msg = await ws.recv()
                    res = json.loads(msg)
                    data = format_data(res)

                    producer.send('depth_automation', json.dumps(data).encode('utf-8'))
                except Exception as e:
                    logging.error(e)
                    continue
    finally:
        producer.flush()
        producer.close()

def run_stream_data():
    asyncio.run(stream_data())

with DAG(
    dag_id='depth_automation',
    default_args=default_args,
    schedule_interval='@daily',
    description='Daily depth automation',
    catchup=False
) as dag:
    streaming_task = PythonOperator(
        task_id='stream_depth_from_api',
        python_callable=run_stream_data,
    )

    streaming_task