import json
from datetime import datetime, tzinfo
from typing import Iterable

from dateutil.tz import tz, tzutc
from pyflink.datastream import ProcessWindowFunction

from visit import Visit


class VisitWindowProcessor(ProcessWindowFunction):
    def process(self, browser: str, context: 'ProcessWindowFunction.Context',
                elements: Iterable[Visit]) -> Iterable[str]:
        window_start = datetime.fromtimestamp(context.window().start / 1000, tz=tzutc()).isoformat()
        window_end = datetime.fromtimestamp(context.window().end / 1000, tz=tzutc()).isoformat()
        return [json.dumps({
            'browser': browser,
            'count': len(elements),
            'window_start': window_start,
            'window_end': window_end
        })]