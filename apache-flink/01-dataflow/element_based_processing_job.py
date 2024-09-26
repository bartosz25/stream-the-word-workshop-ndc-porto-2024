import datetime
import json
import os
import time

from pyflink.common import SimpleStringSchema, WatermarkStrategy, Configuration
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, KeyedStream, KeyedProcessFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

# This configuration is very important
# Without it, you may encounter class loading-related errors, such as:
# Caused by: java.lang.ClassCastException: cannot assign instance of
#   org.apache.kafka.clients.consumer.OffsetResetStrategy to field
#   org.apache.flink.connector.kafka.source.enumerator.initializer.ReaderHandledOffsetsInitializer.offsetResetStrategy
#   of type org.apache.kafka.clients.consumer.OffsetResetStrategy
#   in instance of org.apache.flink.connector.kafka.source.enumerator.initializer.ReaderHandledOffsetsInitializer
config = Configuration()
config.set_string("classloader.resolve-order", "parent-first")
config.set_string("rest.port", "4646")

env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.set_parallelism(2)
# Unlike PySpark, you have to define the JARs explicitly for Flink
env.add_jars(
    f"file://{os.getcwd()}/kafka-clients-3.2.3.jar",
    f"file://{os.getcwd()}/flink-connector-base-1.19.0.jar",
    f"file://{os.getcwd()}/flink-connector-kafka-3.2.0-1.19.jar"
)

kafka_source = (KafkaSource.builder().set_bootstrap_servers('localhost:9094')
    .set_group_id('ndc_demo')
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(SimpleStringSchema()).set_topics('visits').build())

data_source = env.from_source(source=kafka_source, watermark_strategy=WatermarkStrategy.no_watermarks(),
                              source_name="Kafka Source")


def decorate_record_with_utc_prefix(record: str) -> str:
    return f'{datetime.datetime.utcnow().isoformat()} >>> {record}'

def get_browser_group_key(visit_json: str) -> str:
    visit_dict = json.loads(visit_json)
    browser = visit_dict['context']['technical']['browser']
    if browser == 'Chrome':
        return 'chrome'
    else:
        return 'other'

even_odd_visits: KeyedStream = data_source.key_by(get_browser_group_key, Types.STRING())

class BlockingProcessingTimeDecorator(KeyedProcessFunction):

    def process_element(self, record, ctx: 'KeyedProcessFunction.Context'):
        current_key_is_even: str = ctx.get_current_key()
        if current_key_is_even == 'chrome':
            time.sleep(100000)
        else:
            yield f'{datetime.datetime.utcnow().isoformat()} >>> {record}'


decorated_records = even_odd_visits.process(BlockingProcessingTimeDecorator(), Types.STRING())


decorated_records.print()

env.execute('DataFlow model')
