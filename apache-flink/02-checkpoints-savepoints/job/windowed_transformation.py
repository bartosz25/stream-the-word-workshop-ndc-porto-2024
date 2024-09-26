import dataclasses
import json
import datetime
from typing import Iterable

from pyflink.datastream import ProcessWindowFunction


@dataclasses.dataclass
class Visit:
    visit_id: int
    event_time: int
    page: str

def map_json_to_visit(json_payload: str) -> Visit:
    event = json.loads(json_payload)
    event_time = int(datetime.datetime.fromisoformat(event['event_time']).timestamp())
    print(f'event time was {event_time}')
    return Visit(visit_id=event['visit_id'], page=event['page'], event_time=event_time)

class VisitWindowProcessor(ProcessWindowFunction):
    def process(self, visit_id: str, context: 'ProcessWindowFunction.Context',
                elements: Iterable[Visit]) -> Iterable[str]:
        window_start = datetime.datetime.fromtimestamp(context.window().start / 1000).isoformat()
        window_end = datetime.datetime.fromtimestamp(context.window().end / 1000).isoformat()
        visits = len(elements)
        return [f">> VISIT{visit_id}, range=[{window_start} to {window_end}], visits={visits}"]