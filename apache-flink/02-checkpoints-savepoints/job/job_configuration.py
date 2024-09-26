from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, TimeCharacteristic


def configure_the_job_environment() -> StreamExecutionEnvironment:
    # This configuration is very important
    # Without it, you may encounter class loading-related errors, such as:
    # Caused by: java.lang.ClassCastException: cannot assign instance of
    #   org.apache.kafka.clients.consumer.OffsetResetStrategy to field
    #   org.apache.flink.connector.kafka.source.enumerator.initializer.ReaderHandledOffsetsInitializer.offsetResetStrategy
    #   of type org.apache.kafka.clients.consumer.OffsetResetStrategy
    #   in instance of org.apache.flink.connector.kafka.source.enumerator.initializer.ReaderHandledOffsetsInitializer
    config = Configuration()
    config.set_string("classloader.resolve-order", "parent-first")

    stream_environment: StreamExecutionEnvironment = StreamExecutionEnvironment.get_execution_environment(configuration=config)
    stream_environment.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    stream_environment.set_parallelism(2)
    stream_environment.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    stream_environment.add_jars(
        'file:///opt/flink/usrlib/job/kafka-clients-3.2.3.jar',
        'file:///opt/flink/usrlib/job/flink-connector-base-1.17.0.jar',
        'file:///opt/flink/usrlib/job/flink-connector-kafka-1.17.0.jar',
    )
    stream_environment.get_config().set_auto_watermark_interval(5000)

    return stream_environment