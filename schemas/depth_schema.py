from pyspark.sql.types import *

def create_depth_schema():
    depth_schema = StructType([
        StructField('symbol', StringType(), False),
        StructField('event_time', LongType(), True),
        StructField('first_update_id', LongType(), True),
        StructField('last_update_id', LongType(), True),
        StructField('best_bid_price', FloatType(), True),
        StructField('best_bid_qty', FloatType(), True),
        StructField('best_ask_price', FloatType(), True),
        StructField('best_ask_qty', FloatType(), True),
        StructField('event_time_ts', TimestampType(), True)
    ])

    return depth_schema