from pyflink.common import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import (DataStream, WindowedStream)
from pyflink.datastream.connectors.kafka import KafkaSink
from pyflink.datastream.window import TumblingEventTimeWindows, CountTrigger

from windowed_transformation import VisitWindowProcessor, map_json_to_visit


def generate_visit_windows(kafka_data_stream: DataStream, kafka_sink: KafkaSink):
    records_per_visit_counts: WindowedStream = (kafka_data_stream.map(map_json_to_visit)
        .key_by(lambda visit: visit.visit_id).window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .allowed_lateness(0)
        .trigger(CountTrigger.of(4)))

    window_output: DataStream = records_per_visit_counts.process(VisitWindowProcessor(), Types.STRING()).uid(
        "window output")

    window_output.sink_to(kafka_sink)