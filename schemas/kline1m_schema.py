from pyspark.sql.types import *

def create_kline1m_schema():
    kline1m_schema = StructType([
        StructField('symbol', StringType(), False),
        StructField('event_time', LongType(), True),
        StructField('start_time', LongType(), True),
        StructField('close_time', LongType(), True),
        StructField('open', FloatType(), True),
        StructField('close', FloatType(), True),
        StructField('high', FloatType(), True),
        StructField('low', FloatType(), True),
        StructField('volume', FloatType(), True),
        StructField('num_trades', IntegerType(), True),
        StructField('taker_buy_quote_volume', FloatType(), True),
        StructField('is_closed', BooleanType(), True),
        StructField('price_change_percent', FloatType(), True),
        StructField('volatility', FloatType(), True),
        StructField('event_time_ts', TimestampType(), True),
        StructField('start_time_ts', TimestampType(), True),
        StructField('close_time_ts', TimestampType(), True)
    ])

    return kline1m_schema