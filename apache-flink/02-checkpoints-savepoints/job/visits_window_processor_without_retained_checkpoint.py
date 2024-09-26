from pyflink.datastream import (StreamExecutionEnvironment, DataStream, CheckpointingMode,
                                ExternalizedCheckpointCleanup)
from pyflink.datastream.connectors.kafka import KafkaSource

from io_builders import build_kafka_source, convert_kafka_source_to_data_stream, build_kafka_sink
from job_configuration import configure_the_job_environment
from window_generator import generate_visit_windows

if __name__ == "__main__":
    job_id = 'no_checkpointed_consumer'

    stream_environment: StreamExecutionEnvironment = configure_the_job_environment()
    # It's here where we configure the checkpoint frequency
    checkpoint_interval_30_seconds = 30000
    stream_environment.enable_checkpointing(checkpoint_interval_30_seconds, mode=CheckpointingMode.AT_LEAST_ONCE)
    (stream_environment.get_checkpoint_config()
     .set_externalized_checkpoint_cleanup(ExternalizedCheckpointCleanup.NO_EXTERNALIZED_CHECKPOINTS))

    kafka_source: KafkaSource = build_kafka_source(group_id=job_id)

    data_source: DataStream = convert_kafka_source_to_data_stream(kafka_source=kafka_source,
                                                                  stream_environment=stream_environment)

    generate_visit_windows(kafka_data_stream=data_source, kafka_sink=build_kafka_sink())

    stream_environment.execute(f'Job id: {job_id}')
