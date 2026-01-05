import logging
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from schemas.ticker_schema import create_ticker_schema
from schemas.trade_schema import create_trade_schema
from schemas.depth_schema import create_depth_schema
from schemas.bookTicker_schema import create_bookTicker_schema
from schemas.kline1m_schema import create_kline1m_schema


def create_spark_connection():
    spark_conn = None

    try:
        spark_conn = SparkSession.builder \
            .appName("Spark Streaming") \
            .config("spark.jars",
                    "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.4.2.jar,"
                    "/opt/spark/jars/kafka-clients-3.4.0.jar,"
                    "/opt/spark/jars/elasticsearch-spark-30_2.12-8.11.3.jar") \
            .config("es.nodes", "elasticsearch") \
            .config("es.port", "9200") \
            .config("es.nodes.wan.only", "true") \
            .config("es.index.auto.create", "true") \
            .getOrCreate()

        spark_conn.sparkContext.setLogLevel('ERROR')
        logging.info('Spark connected successfully')
    except Exception as e:
        logging.error(f'Could not connect to Spark. Error: {e}')

    return spark_conn

# topics = "ticker_automation,trade_automation,kline_1m_automation,depth_automation,bookTicker_automation"

def read_kafka_topic(spark_conn, topic):
    spark_df = None

    try:
        spark_df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers","broker:29092") \
            .option("subscribe",topic) \
            .option("failOnDataLoss", "false") \
            .load()

        logging.info(f'Connected to Kafka topic: {topic}')

    except Exception as e:
        logging.error(f'Could not connect to {topic} Kafka topic). Error: {e}')

    return spark_df

def parse_df(spark_df, schema):
    select_expr = spark_df.selectExpr("CAST(value as STRING)") \
                          .select(from_json(col('value'), schema).alias("data")) \
                          .select("data.*")

    return select_expr

def write_to_es(df, index_name, checkpoint_path):
    return(
        df.writeStream
            .format("org.elasticsearch.spark.sql")
            .option("es.nodes", "elasticsearch")
            .option("es.port", "9200")
            .option("es.nodes.wan.only", "true")
            .option("checkpointLocation", checkpoint_path)
            .outputMode("append")
            .start(index_name)
    )

if __name__ == '__main__':
    #create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        df_ticker_raw = read_kafka_topic(spark_conn, "ticker_automation")
        df_trade_raw = read_kafka_topic(spark_conn, "trade_automation")
        df_depth_raw = read_kafka_topic(spark_conn, "depth_automation")
        df_bookTicker_raw = read_kafka_topic(spark_conn, "bookTicker_automation")
        df_kline1m_raw = read_kafka_topic(spark_conn, "kline1m_automation")

        df_ticker = parse_df(df_ticker_raw, create_ticker_schema())
        df_trade = parse_df(df_trade_raw, create_trade_schema())
        df_depth = parse_df(df_depth_raw, create_depth_schema())
        df_bookTicker = parse_df(df_bookTicker_raw, create_bookTicker_schema())
        df_kline1m = parse_df(df_kline1m_raw, create_kline1m_schema())

        df_ticker = df_ticker.withColumn(
            "spread",
            col("best_ask_price") - col("best_bid_price")
        ).withColumn(
            "event_time_ts",
            F.date_format(
                F.to_timestamp(col("event_time") / 1000),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            )
        )

        df_trade = df_trade.withColumn(
            "trade_value",
            col("price") * col("quantity")
        ).withColumn(
            "trade_time_ts",
            F.date_format(
                F.to_timestamp(col("trade_time") / 1000),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            )
        )

        df_depth = df_depth.withColumn(
            "event_time_ts",
            F.date_format(
                F.to_timestamp(col("event_time") / 1000),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            )
        )

        df_bookTicker = df_bookTicker.withColumn(
            "spread",
            col("best_ask_price") - col("best_bid_price")
        )

        df_kline1m = df_kline1m.withColumn(
            "price_change_percent",
            (col("close") - col("open")) / col("open")
        ).withColumn(
            "volatility",
            col("high") - col("low")
        ).withColumn(
            "event_time_ts",
            F.date_format(
                F.to_timestamp(col("event_time") / 1000),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            )
        ).withColumn(
            "start_time_ts",
            F.date_format(
                F.to_timestamp(col("start_time") / 1000),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            )
        ).withColumn(
            "close_time_ts",
            F.date_format(
                F.to_timestamp(col("close_time") / 1000),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            )
        )

        queries = [
            write_to_es(df_ticker, "ticker_index", "/tmp/checkpoints/ticker"),
            write_to_es(df_trade, "trade_index", "/tmp/checkpoints/trade"),
            write_to_es(df_depth, "depth_index", "/tmp/checkpoints/depth"),
            write_to_es(df_bookTicker, "bookticker_index", "/tmp/checkpoints/bookticker"),
            write_to_es(df_kline1m, "kline1m_index", "/tmp/checkpoints/kline1m")
        ]

        for q in queries:
            q.awaitTermination()

