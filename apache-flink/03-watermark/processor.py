import dataclasses
import json
from datetime import datetime, timezone
from typing import Any, Dict

from pyflink.datastream import ProcessFunction, OutputTag


@dataclasses.dataclass
class WrappedVisit:
    original_payload: str
    processing_time_epoch_seconds: int
    event_time_epoch_ms: int

    def to_dict(self, latency: int) -> Dict[str, Any]:
        base_visit = {
            'visit_stringified': self.original_payload,
            'processing_time_seconds': (datetime.fromtimestamp(self.processing_time_epoch_seconds, timezone.utc)
                                        .isoformat(sep='T'))
        }
        if latency > 0:
            base_visit['latency'] = latency

        return base_visit


class WrappedVisitDataProcessor(ProcessFunction):

    def __init__(self, late_data_output: OutputTag):
        self.late_data_output = late_data_output

    def process_element(self, value: WrappedVisit, ctx: 'ProcessFunction.Context'):
        current_watermark = ctx.timer_service().current_watermark()
        latency = current_watermark - value.event_time_epoch_ms
        json_to_dispatch = json.dumps(value.to_dict(latency))
        if latency > 0:
            yield self.late_data_output, json_to_dispatch
        else:
            yield json_to_dispatch
