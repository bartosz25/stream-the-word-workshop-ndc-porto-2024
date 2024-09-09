import datetime
import json
import os

from pyflink.common import SimpleStringSchema, WatermarkStrategy, Configuration, Duration, Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, DataStream, \
    TimeCharacteristic, WindowedStream
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema
from pyflink.datastream.window import TumblingEventTimeWindows

from assigner import VisitTimestampAssigner
from processor import VisitWindowProcessor
from trigger import PartialEventTimeWindowTrigger
from visit import Visit

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

config.set_boolean("python.operator-chaining.enabled", False)
env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.set_parallelism(2)
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
# Unlike PySpark, you have to define the JARs explicitly for Flink
env.add_jars(
    f"file://{os.getcwd()}/kafka-clients-3.2.3.jar",
    f"file://{os.getcwd()}/flink-connector-base-1.19.0.jar",
    f"file://{os.getcwd()}/flink-connector-kafka-3.2.0-1.19.jar"
)
env.get_config().set_auto_watermark_interval(5000)


kafka_source = (KafkaSource.builder().set_bootstrap_servers('localhost:9094')
    .set_group_id('wfc_partial_trigger')
    .set_starting_offsets(KafkaOffsetsInitializer.latest())
    .set_value_only_deserializer(SimpleStringSchema()).set_topics('visits').build())

watermark_strategy = (WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(30))
                      .with_timestamp_assigner(VisitTimestampAssigner()))

data_source = (env.from_source(source=kafka_source, watermark_strategy=watermark_strategy, source_name="Kafka Source")
               .assign_timestamps_and_watermarks(watermark_strategy))#.disable_chaining()

def map_json_to_visit(json_payload: str) -> Visit:
    event = json.loads(json_payload)
    event_time = int(datetime.datetime.fromisoformat(event['event_time']).timestamp())
    return Visit(visit_id=event['visit_id'], browser=event['browser'], event_time=event_time)


records_per_visit_counts: WindowedStream = (data_source.map(map_json_to_visit)
    .key_by(lambda visit: visit.browser).window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .allowed_lateness(0).trigger(PartialEventTimeWindowTrigger()))

# must add the Types.STRING() to avoid serialization issues
window_output: DataStream = records_per_visit_counts.process(VisitWindowProcessor(), Types.STRING()).uid(
    "window output")

stats_sink: KafkaSink = (KafkaSink.builder().set_bootstrap_servers("localhost:9094")
                                  .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                                         .set_topic('visits-stats-partial-trigger')
                                                         .set_value_serialization_schema(SimpleStringSchema())
                                                         .build())
                                  .build())

window_output.sink_to(stats_sink)


env.execute('Trigger demo')
