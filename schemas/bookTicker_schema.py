from pyspark.sql.types import *

def create_bookTicker_schema():
    bookTicker_schema = StructType([
        StructField('update_id', LongType(), False),
        StructField('symbol', StringType(), False),
        StructField('best_bid_price', FloatType(), True),
        StructField('best_bid_qty', FloatType(), True),
        StructField('best_ask_price', FloatType(), True),
        StructField('best_ask_qty', FloatType(), True),
        StructField('spread', FloatType(), True)
    ])

    return bookTicker_schema