from pyflink.datastream import Trigger, TriggerResult
from pyflink.datastream.window import W, T, EventTimeTrigger


class PartialEventTimeWindowTrigger(EventTimeTrigger):

    PARTIAL_RESULTS_MILLIS_THRESHOLD_3_MIN_AS_MILLIS = 3 * 60 * 1000

    def on_element(self, element: T, watermark_time: int, window: W, ctx: 'Trigger.TriggerContext') -> TriggerResult:
        remaining_millis_before_window_end = (window.max_timestamp() - watermark_time)
        if remaining_millis_before_window_end <= PartialEventTimeWindowTrigger.PARTIAL_RESULTS_MILLIS_THRESHOLD_3_MIN_AS_MILLIS:
            print(f'Emitting partial results, {remaining_millis_before_window_end} ms remaining before the end of the window')
            return TriggerResult.FIRE
        else:
            return super().on_element(element, watermark_time, window, ctx)