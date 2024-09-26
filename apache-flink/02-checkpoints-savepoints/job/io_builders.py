from pyflink.common import Encoder, SimpleStringSchema, WatermarkStrategy, Duration
from pyflink.datastream import DataStream, StreamExecutionEnvironment, DataStreamSink
from pyflink.datastream.connectors.file_system import FileSink, RollingPolicy, BucketAssigner
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema

from assigner import VisitTimestampAssigner


def build_kafka_source(group_id: str) -> KafkaSource:
    return (KafkaSource.builder().set_bootstrap_servers('kafka:9092')
            .set_group_id(group_id)
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(SimpleStringSchema()).set_topics('visits').build())

def convert_kafka_source_to_data_stream(kafka_source: KafkaSource,
                                        stream_environment: StreamExecutionEnvironment) -> DataStream:
    watermark_strategy = (WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(10))
                          .with_timestamp_assigner(VisitTimestampAssigner()))
    data_source = stream_environment.from_source(source=kafka_source, watermark_strategy=watermark_strategy,
                                                 source_name="Kafka Source"
    ).assign_timestamps_and_watermarks(watermark_strategy).uid("Kafka::visits")

    return data_source

def build_kafka_sink() -> KafkaSink:
    return (KafkaSink.builder().set_bootstrap_servers("kafka:9092")
        .set_record_serializer(KafkaRecordSerializationSchema.builder()
                               .set_topic('visit-windows')
                               .set_value_serialization_schema(SimpleStringSchema()).build())
        .build())

