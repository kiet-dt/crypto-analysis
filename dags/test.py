import asyncio
import json
import websockets
import logging
from datetime import datetime, timedelta

async def get_data():
    url = "wss://stream.binance.com:9443/stream?streams=btcusdt@depth/ethusdt@depth/bnbusdt@depth/solusdt@depth/xrpusdt@depth"

    async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
        print("Connected!")

        msg = await ws.recv()
        res = json.loads(msg)

        print(res)

asyncio.run(get_data())
