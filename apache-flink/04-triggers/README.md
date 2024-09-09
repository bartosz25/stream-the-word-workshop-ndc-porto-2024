# Apache Flink - triggers
 
## Event-time, aka watermark, trigger
1. Explain the [visits_counter_job_watermark_trigger.py](visits_counter_job_watermark_trigger.py)
* the job computes number of events for each browser in 5 minutes windows; the logic is the same for all demos and the
single difference is the `.trigger(...)` method defined at the window level
* here we're using an `EventTimeTrigger` that is going to emit the window only when it'll be older than the current watermark

2. Start Docker containers:
```
cd docker
docker-compose down --volumes; docker-compose up
```

3. Start the console producer:
```
docker exec -ti wfc_kafka kafka-console-producer.sh --broker-list localhost:9092 --topic visits --property parse.key=true --property key.separator==
```

4. Start the console consumer:
```
docker exec -ti wfc_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic visits-stats-watermark-trigger --from-beginning
```

5. Start the `visits_counter_job_watermark_trigger.py`.

6. Emit first records:
```
a={"visit_id": 1, "event_time": "2024-10-06T10:00:00.000+00:00", "browser": "Firefox"}
a={"visit_id": 2, "event_time": "2024-10-06T10:00:00.000+00:00", "browser": "Firefox"}
a={"visit_id": 3, "event_time": "2024-10-06T10:00:00.000+00:00", "browser": "Firefox"}
b={"visit_id": 4, "event_time": "2024-10-06T10:02:00.000+00:00", "browser": "Chrome"}
b={"visit_id": 5, "event_time": "2024-10-06T10:03:00.000+00:00", "browser": "Chrome"}
b={"visit_id": 6, "event_time": "2024-10-06T10:03:00.000+00:00", "browser": "Chrome"}
c={"visit_id": 7, "event_time": "2024-10-06T10:06:00.000+00:00", "browser": "Firefox"}
c={"visit_id": 9, "event_time": "2024-10-06T10:08:00.000+00:00", "browser": "Firefox"}
c={"visit_id": 11, "event_time": "2024-10-06T10:08:00.000+00:00", "browser": "Firefox"}
d={"visit_id": 8, "event_time": "2024-10-06T10:07:00.000+00:00", "browser": "Safari"}
d={"visit_id": 10, "event_time": "2024-10-06T10:08:00.000+00:00", "browser": "Safari"}
d={"visit_id": 11, "event_time": "2024-10-06T10:08:00.000+00:00", "browser": "Safari"}
```

The consumer should emit any window as none of the created ones hasn't crossed the watermark yet.

7. Send the next batch of records:
```
a={"visit_id": 222, "event_time": "2024-10-06T10:07:00.000+00:00", "browser": "Firefox"} 
a={"visit_id": 222, "event_time": "2024-10-06T10:07:00.000+00:00", "browser": "Firefox"} 
a={"visit_id": 222, "event_time": "2024-10-06T10:07:00.000+00:00", "browser": "Firefox"} 
z={"visit_id": 22, "event_time": "2024-10-06T10:09:00.000+00:00", "browser": "Chrome"} 
z={"visit_id": 22, "event_time": "2024-10-06T10:09:00.000+00:00", "browser": "Chrome"} 
z={"visit_id": 22, "event_time": "2024-10-06T10:09:00.000+00:00", "browser": "Chrome"} 
z={"visit_id": 22, "event_time": "2024-10-06T10:09:00.000+00:00", "browser": "Chrome"} 
```

As you can see, despite new records, there are no windows emitted. 

8. Let's see if the windows are emitted after the watermark. To do this, we have to generate records whose watermark will
be earlier than the previous windows' end times:

```
a={"visit_id": 1, "event_time": "2024-10-06T11:40:00.000+00:00", "browser": "Firefox"}
b={"visit_id": 2, "event_time": "2024-10-06T11:43:00.000+00:00", "browser": "Firefox"}
c={"visit_id": 3, "event_time": "2024-10-06T11:46:00.000+00:00", "browser": "Firefox"}
d={"visit_id": 4, "event_time": "2024-10-06T11:46:00.000+00:00", "browser": "Firefox"}
```

They will set the watermark to 11:16 which should result in emitting the pending windows in the console consumer:

```
{"browser": "Firefox", "count": 3, "window_start": "2024-10-06T10:00:00+00:00", "window_end": "2024-10-06T10:05:00+00:00"}
{"browser": "Chrome", "count": 3, "window_start": "2024-10-06T10:00:00+00:00", "window_end": "2024-10-06T10:05:00+00:00"}

{"browser": "Safari", "count": 3, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Chrome", "count": 4, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Firefox", "count": 6, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
```

## Count trigger
1. Explain [visits_counter_job_count_trigger.py](visits_counter_job_count_trigger.py)
* here the trigger will emit partial results, i.e. every 2 new items integrated to the window

2. Start new consumer:
```
docker exec -ti wfc_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic visits-stats-count-trigger --from-beginning
```

3. Start the `visits_counter_job_count_trigger.py`.

4. Emit first records:
```
a={"visit_id": 1, "event_time": "2024-10-06T10:00:00.000+00:00", "browser": "Firefox"}
a={"visit_id": 2, "event_time": "2024-10-06T10:00:00.000+00:00", "browser": "Firefox"}
a={"visit_id": 3, "event_time": "2024-10-06T10:00:00.000+00:00", "browser": "Firefox"}
b={"visit_id": 4, "event_time": "2024-10-06T10:02:00.000+00:00", "browser": "Chrome"}
b={"visit_id": 5, "event_time": "2024-10-06T10:03:00.000+00:00", "browser": "Chrome"}
b={"visit_id": 6, "event_time": "2024-10-06T10:03:00.000+00:00", "browser": "Chrome"}
c={"visit_id": 7, "event_time": "2024-10-06T10:06:00.000+00:00", "browser": "Firefox"}
c={"visit_id": 9, "event_time": "2024-10-06T10:08:00.000+00:00", "browser": "Firefox"}
c={"visit_id": 11, "event_time": "2024-10-06T10:08:00.000+00:00", "browser": "Firefox"}
d={"visit_id": 8, "event_time": "2024-10-06T10:07:00.000+00:00", "browser": "Safari"}
d={"visit_id": 10, "event_time": "2024-10-06T10:08:00.000+00:00", "browser": "Safari"}
d={"visit_id": 11, "event_time": "2024-10-06T10:08:00.000+00:00", "browser": "Safari"}
```

The consumer should emit partial windows with 2 out of 3 produced elements:
```
{"browser": "Safari", "count": 2, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Firefox", "count": 2, "window_start": "2024-10-06T10:00:00+00:00", "window_end": "2024-10-06T10:05:00+00:00"}
{"browser": "Chrome", "count": 2, "window_start": "2024-10-06T10:00:00+00:00", "window_end": "2024-10-06T10:05:00+00:00"}
{"browser": "Firefox", "count": 2, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
```

5. Send the next batch of records:
```
a={"visit_id": 222, "event_time": "2024-10-06T10:07:00.000+00:00", "browser": "Firefox"} 
a={"visit_id": 222, "event_time": "2024-10-06T10:07:00.000+00:00", "browser": "Firefox"} 
a={"visit_id": 222, "event_time": "2024-10-06T10:07:00.000+00:00", "browser": "Firefox"} 
z={"visit_id": 22, "event_time": "2024-10-06T10:09:00.000+00:00", "browser": "Chrome"} 
z={"visit_id": 22, "event_time": "2024-10-06T10:09:00.000+00:00", "browser": "Chrome"} 
z={"visit_id": 22, "event_time": "2024-10-06T10:09:00.000+00:00", "browser": "Chrome"} 
z={"visit_id": 22, "event_time": "2024-10-06T10:09:00.000+00:00", "browser": "Chrome"} 
```

As you can see in the consumer's console, there should be extra partial windows emitted:
```
{"browser": "Firefox", "count": 4, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Chrome", "count": 4, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Firefox", "count": 6, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Chrome", "count": 2, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
```

6. Let's add an extra record for the Firefox browser that should be included in the window 10:05-10:10:
```
a={"visit_id": 222, "event_time": "2024-10-06T10:07:00.000+00:00", "browser": "Firefox"} 
```

7. The job hasn't emitted the update as it triggers every 2 elements. Let's move now the watermark to see if the window
is emitted despite this incomplete count status.
```
a={"visit_id": 1, "event_time": "2024-10-06T11:40:00.000+00:00", "browser": "Firefox"}
b={"visit_id": 2, "event_time": "2024-10-06T11:43:00.000+00:00", "browser": "Firefox"}
c={"visit_id": 3, "event_time": "2024-10-06T11:46:00.000+00:00", "browser": "Firefox"}
d={"visit_id": 4, "event_time": "2024-10-06T11:46:00.000+00:00", "browser": "Firefox"}
```
 
As you can see, the job only generated windows for the new windows, and so despite the watermark change:
```
{"browser": "Firefox", "count": 2, "window_start": "2024-10-06T11:40:00+00:00", "window_end": "2024-10-06T11:45:00+00:00"}
{"browser": "Firefox", "count": 2, "window_start": "2024-10-06T11:45:00+00:00", "window_end": "2024-10-06T11:50:00+00:00"}
```

8. Let's add now the same record for Firefox as before but as the watermark moved on, the window is not present anymore:
```
a={"visit_id": 222, "event_time": "2024-10-06T10:07:00.000+00:00", "browser": "Firefox"} 
```

Consequently, due to the count-based trigger semantics, we lost the update from the point 6.

## Custom window completeness trigger
1. Explain [visits_counter_job_custom_partial_trigger.py](visits_counter_job_custom_partial_trigger.py)
* it's an evolution combining partial results emission and the final, watermark-based
* the code uses [trigger.py](trigger.py)
  * the trigger emits either partial results if the last even integrated to the window occurred at least 3 minutes before
  the window end time, or the final results if the watermark passes by

2. Start a new console consumer:
```
docker exec -ti wfc_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic visits-stats-partial-trigger --from-beginning
```

3. Start the `visits_counter_job_custom_partial_trigger.py`

4. We're going to repeat the same steps as in the previous trigger's demo. Spoiler alert, this time we shouldn't lost any
record which is within the watermark boundary. Let's start with the first bunch of events:
```
a={"visit_id": 1, "event_time": "2024-10-06T10:00:00.000+00:00", "browser": "Firefox"}
a={"visit_id": 2, "event_time": "2024-10-06T10:00:00.000+00:00", "browser": "Firefox"}
a={"visit_id": 3, "event_time": "2024-10-06T10:00:00.000+00:00", "browser": "Firefox"}
b={"visit_id": 4, "event_time": "2024-10-06T10:02:00.000+00:00", "browser": "Chrome"}
b={"visit_id": 5, "event_time": "2024-10-06T10:03:00.000+00:00", "browser": "Chrome"}
b={"visit_id": 6, "event_time": "2024-10-06T10:03:00.000+00:00", "browser": "Chrome"}
c={"visit_id": 7, "event_time": "2024-10-06T10:06:00.000+00:00", "browser": "Firefox"}
c={"visit_id": 9, "event_time": "2024-10-06T10:08:00.000+00:00", "browser": "Firefox"}
c={"visit_id": 11, "event_time": "2024-10-06T10:08:00.000+00:00", "browser": "Firefox"}
d={"visit_id": 8, "event_time": "2024-10-06T10:07:00.000+00:00", "browser": "Safari"}
d={"visit_id": 10, "event_time": "2024-10-06T10:08:00.000+00:00", "browser": "Safari"}
d={"visit_id": 11, "event_time": "2024-10-06T10:08:00.000+00:00", "browser": "Safari"}
```

The consumer should generate partial windows:
```
{"browser": "Chrome", "count": 1, "window_start": "2024-10-06T10:00:00+00:00", "window_end": "2024-10-06T10:05:00+00:00"}
{"browser": "Chrome", "count": 2, "window_start": "2024-10-06T10:00:00+00:00", "window_end": "2024-10-06T10:05:00+00:00"}
{"browser": "Chrome", "count": 3, "window_start": "2024-10-06T10:00:00+00:00", "window_end": "2024-10-06T10:05:00+00:00"}
{"browser": "Firefox", "count": 2, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Firefox", "count": 3, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Safari", "count": 1, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Safari", "count": 2, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Safari", "count": 3, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
```

⚠️ As you can notice, our implementation emits each update, i.e. whenever there are 3 minutes left to the end of the window,
the job sends each partial increments to the output topic.

5. Send the next batch of records:
```
a={"visit_id": 222, "event_time": "2024-10-06T10:07:00.000+00:00", "browser": "Firefox"} 
a={"visit_id": 222, "event_time": "2024-10-06T10:07:00.000+00:00", "browser": "Firefox"} 
a={"visit_id": 222, "event_time": "2024-10-06T10:07:00.000+00:00", "browser": "Firefox"} 
z={"visit_id": 22, "event_time": "2024-10-06T10:09:00.000+00:00", "browser": "Chrome"} 
z={"visit_id": 22, "event_time": "2024-10-06T10:09:00.000+00:00", "browser": "Chrome"} 
z={"visit_id": 22, "event_time": "2024-10-06T10:09:00.000+00:00", "browser": "Chrome"} 
z={"visit_id": 22, "event_time": "2024-10-06T10:09:00.000+00:00", "browser": "Chrome"} 
```

As you can see in the consumer's console, there should be extra partial windows emitted:
```
{"browser": "Firefox", "count": 4, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Firefox", "count": 5, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Firefox", "count": 6, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Chrome", "count": 1, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Chrome", "count": 2, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Chrome", "count": 3, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Chrome", "count": 4, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
```

6. Let's add an extra record for the Firefox browser that should be included in the window 10:05-10:10:
```
a={"visit_id": 222, "event_time": "2024-10-06T10:07:00.000+00:00", "browser": "Firefox"} 
```

This time, the window should be updated:
```
{"browser": "Firefox", "count": 7, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
```

7. Let's move now the watermark to see if the windows are emitted too:
```
a={"visit_id": 1, "event_time": "2024-10-06T11:40:00.000+00:00", "browser": "Firefox"}
b={"visit_id": 2, "event_time": "2024-10-06T11:43:00.000+00:00", "browser": "Firefox"}
c={"visit_id": 3, "event_time": "2024-10-06T11:46:00.000+00:00", "browser": "Firefox"}
d={"visit_id": 4, "event_time": "2024-10-06T11:46:00.000+00:00", "browser": "Firefox"}
```
 
As you can see, we get the result for one new window (the 1st line) and all final results for the remaining windows:
```
{"browser": "Firefox", "count": 2, "window_start": "2024-10-06T11:40:00+00:00", "window_end": "2024-10-06T11:45:00+00:00"}
{"browser": "Firefox", "count": 3, "window_start": "2024-10-06T10:00:00+00:00", "window_end": "2024-10-06T10:05:00+00:00"}
{"browser": "Chrome", "count": 3, "window_start": "2024-10-06T10:00:00+00:00", "window_end": "2024-10-06T10:05:00+00:00"}
{"browser": "Safari", "count": 3, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Chrome", "count": 4, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}
{"browser": "Firefox", "count": 7, "window_start": "2024-10-06T10:05:00+00:00", "window_end": "2024-10-06T10:10:00+00:00"}

```

8. Let's prove now that the watermark moved on and we're not going to integrate the record into a closed window:
```
a={"visit_id": 222, "event_time": "2024-10-06T10:07:00.000+00:00", "browser": "Firefox"} 
```

You shouldn't see any update in the consumer's tab.