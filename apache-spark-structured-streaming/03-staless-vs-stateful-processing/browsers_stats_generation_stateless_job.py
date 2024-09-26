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

    windows_with_browser_groups = (input_data_stream
        .select(F.from_json(F.col('value').cast('string'), 'eventTime TIMESTAMP, browser STRING').alias('value2'), 'value')
        .selectExpr('value2.*', 'value'))

    def write_stateless_aggregrations(dataframe_to_aggregate: DataFrame, batch_number: int):
        dataframe_to_aggregate.show()
        windows_with_browser_aggregation = (dataframe_to_aggregate
         .groupBy('browser', F.window(F.col("eventTime"), "5 minutes"))
         .agg(F.max('eventTime'), F.count('browser'))
         .selectExpr('browser', 'window', '`count(browser)` AS count', '`MAX(eventTime)` AS last_event_time_in_window')
         .select(F.to_json(F.struct('*')).alias('value')))

        (windows_with_browser_aggregation.write.format('kafka')
         .option('kafka.bootstrap.servers', 'localhost:9094')
         .option('topic', 'browser-stats-stateless').save())

    write_data_stream = (windows_with_browser_groups.writeStream.foreachBatch(write_stateless_aggregrations)
                         .trigger(processingTime='15 seconds')
                         .option('checkpointLocation',f'{BASE_DIR}/stateless/checkpoint'))

    write_data_stream.start().awaitTermination()
