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
                                    .selectExpr('visit.*'))

    def print_visit_rows_from_foreach_partition(visits: DataFrame, batch_number: int):
        print('✍️ Printing visits')
        def print_visits(visits_to_print: Iterator[Row]):
            prefix = f'Partition[{TaskContext.get().partitionId()}]'
            for visit in visits_to_print:
                print(f'{prefix} > Printing visit={visit}')

        visits.foreachPartition(print_visits)


    write_data_stream = (visits_from_kafka.writeStream
                         .trigger(processingTime='20 seconds')
                         .option('checkpointLocation',f'{BASE_DIR}/checkpoint')
                         .foreachBatch(print_visit_rows_from_foreach_partition))

    write_data_stream.start().awaitTermination()
