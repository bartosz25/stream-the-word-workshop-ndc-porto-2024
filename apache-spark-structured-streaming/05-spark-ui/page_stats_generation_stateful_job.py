from typing import Iterator

from pyspark import Row, TaskContext
from pyspark.sql import SparkSession, functions as F, DataFrame

from config import BASE_DIR

if __name__ == '__main__':
    spark = (SparkSession.builder.master('local[*]')
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate())

    input_data_stream = (spark.readStream
        .option('kafka.bootstrap.servers', 'localhost:9094')
        .option('subscribe', 'visits')
        .option('startingOffsets', 'EARLIEST')
        .format('kafka').load())

    visit_schema = 'event_time TIMESTAMP, page STRING'
    windows_with_browser_groups = (input_data_stream
        .select(F.from_json(F.col('value').cast('string'), visit_schema).alias('value'))
        .selectExpr('value.*')
        .withWatermark('event_time', '20 minutes')
        .groupBy('page', F.window(F.col("event_time"), "5 minutes")))

    windows_with_browser_aggregation = windows_with_browser_groups.agg(F.max('event_time'), F.count('page'))

    output_dataframe = (windows_with_browser_aggregation.selectExpr('page', 'window',
                                                                   '`count(page)` AS count',
                                                                    '`MAX(event_time)` AS last_event_time_in_window')
                        .select(F.to_json(F.struct('*')).alias('value')))


    write_data_stream = (output_dataframe.writeStream.format('kafka')
                         .outputMode('update')
                         .option('kafka.bootstrap.servers', 'localhost:9094')
                         .option('topic', 'page-stats')
                         .trigger(processingTime='15 seconds')
                         .option('checkpointLocation',f'{BASE_DIR}/checkpoint'))

    write_data_stream.start().awaitTermination()
