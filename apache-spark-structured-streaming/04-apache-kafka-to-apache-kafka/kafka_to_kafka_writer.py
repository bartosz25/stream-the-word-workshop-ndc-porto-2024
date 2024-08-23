from typing import Iterator

from pyspark import Row, TaskContext
from pyspark.sql import SparkSession, functions as F, DataFrame

from config import BASE_DIR

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    input_data_stream = (spark.readStream
        .option('kafka.bootstrap.servers', 'localhost:9094')
        .option('subscribe', 'visits')
        .option('startingOffsets', 'EARLIEST')
        .format('kafka').load())

    visit_schema = 'visit_id STRING, event_time TIMESTAMP, page STRING'

    visits_from_kafka: DataFrame = (input_data_stream
                                    .select(F.from_json(F.col('value').cast('string'), visit_schema).alias('visit'),
                                            'value')
                                    .selectExpr('visit.*')
                                    .select(F.to_json(F.struct('*')).alias('value')))


    write_data_stream = (visits_from_kafka.writeStream.format('kafka')
                         .option('kafka.bootstrap.servers', 'localhost:9094')
                         .option('topic', 'reduced-visits')
                         .trigger(processingTime='15 seconds')
                         .option('checkpointLocation',f'{BASE_DIR}/checkpoint'))

    write_data_stream.start().awaitTermination()
