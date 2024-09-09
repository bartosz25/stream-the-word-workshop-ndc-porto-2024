import datetime
import json
import os

from pyflink.common import SimpleStringSchema, WatermarkStrategy, Configuration, Duration
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, OutputTag, DataStream, \
    TimeCharacteristic
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema

from assigner import VisitTimestampAssigner
from processor import WrappedVisitDataProcessor, WrappedVisit

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
    .set_group_id('ndc_demo')
    .set_starting_offsets(KafkaOffsetsInitializer.latest())
    .set_value_only_deserializer(SimpleStringSchema()).set_topics('visits').build())

watermark_strategy = (WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(30))
                      .with_timestamp_assigner(VisitTimestampAssigner()))

data_source = (env.from_source(source=kafka_source, watermark_strategy=watermark_strategy, source_name="Kafka Source")
               .assign_timestamps_and_watermarks(watermark_strategy)).disable_chaining()


late_data_output: OutputTag = OutputTag('late_visits', Types.STRING())

def wrap_the_visit(visit_json: str) -> WrappedVisit:
    event = json.loads(visit_json)
    return WrappedVisit(
        original_payload=visit_json,
        event_time_epoch_ms=int(datetime.datetime.fromisoformat(event['event_time']).timestamp()) * 1000,
        processing_time_epoch_seconds=int(datetime.datetime.utcnow().timestamp())
    )

visits: DataStream = data_source.map(wrap_the_visit).process(WrappedVisitDataProcessor(late_data_output), Types.STRING())

on_time_visits_sink: KafkaSink = (KafkaSink.builder().set_bootstrap_servers("localhost:9094")
                                  .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                                         .set_topic('visits-on-time')
                                                         # you can also use .set_topic_selector() instead of the side output
                                                         .set_value_serialization_schema(SimpleStringSchema())
                                                         .build())
                                  .build())

visits.sink_to(on_time_visits_sink)

late_visits_sink: KafkaSink = (KafkaSink.builder().set_bootstrap_servers("localhost:9094")
                                  .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                                         .set_topic('visits-late')
                                                         .set_value_serialization_schema(SimpleStringSchema())
                                                         .build())
                                  .build())

visits.get_side_output(late_data_output).sink_to(late_visits_sink)

env.execute('Watermark demo')
