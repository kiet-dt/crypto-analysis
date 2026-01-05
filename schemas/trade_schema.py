from pyspark.sql.types import *

def create_trade_schema():
    trade_schema = StructType([
        StructField('symbol', StringType(), False),
        StructField('price', FloatType(), True),
        StructField('quantity', FloatType(), True),
        StructField('trade_time', LongType(), True),
        StructField('buyer_is_maker', BooleanType(), True),
        StructField('trade_time_ts', TimestampType(), True),
        StructField('trade_value', FloatType(), True)
    ])

    return trade_schema