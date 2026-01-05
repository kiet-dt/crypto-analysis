from pyspark.sql.types import *

def create_ticker_schema():
    ticker_schema = StructType([
        StructField('symbol', StringType(), False),
        StructField('open', FloatType(), True),
        StructField('close', FloatType(), True),
        StructField('high', FloatType(), True),
        StructField('low', FloatType(), True),
        StructField('price_change_percent', FloatType(), True),
        StructField('volume', FloatType(), True),
        StructField('quote_volume', FloatType(), True),
        StructField('best_bid_price', FloatType(), True),
        StructField('best_ask_price', FloatType(), True),
        StructField('num_trades', IntegerType(), True),
        StructField('event_time', LongType(), True),
        StructField('event_time_ts', TimestampType(), True),
        StructField('spread', FloatType(), True)
    ])

    return ticker_schema