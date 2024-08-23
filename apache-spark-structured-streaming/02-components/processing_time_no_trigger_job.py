from typing import Iterator

from pyspark import Row, TaskContext
from pyspark.sql import SparkSession, functions as F, DataFrame

from config import BASE_DIR

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').getOrCreate()

    input_data_stream = (spark.readStream
        .option('maxFilesPerTrigger', 3)
        .format('text').load(path=f'{BASE_DIR}/input'))

    write_data_stream = (input_data_stream.writeStream
                         .option('checkpointLocation',f'{BASE_DIR}/no-trigger/checkpoint')
                         .format('console'))

    write_data_stream.start().awaitTermination()
