# Apache Spark Structured Streaming - stateless vs. stateful jobs
 
1. Explain two jobs that apparently will be doing the same thing but in practice, the implementation and the outcome
will be both different:
* [browsers_stats_generation_stateless_job.py](browsers_stats_generation_stateless_job.py) the job generates statistics
by event time browser windows but it limits the scope to the current micro-batch
* [browsers_stats_generation_stateful_job.py](browsers_stats_generation_stateful_job.py) it also generates windowed
statistics but it does it incrementally, i.e. results from the micro-batch _n_ are the input for the results computed in
the micro-batch _n+1_; therefore, we keep some state between them, hence  _statefull_

2. Start the Docker images with the visits data generator:
```
cd docker
docker-compose down --volumes; docker-compose up
```

3. Run `browsers_stats_generation_stateless_job`. 

4. Start Apache Kafka producer:
```
docker exec -ti wfc_kafka kafka-console-producer.sh --broker-list localhost:9092 --topic visits
```

Generate first 3 visits:
```
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:00:00Z", "browser": "Firefox"}
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:01:00Z", "browser": "Chrome"}
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:02:00Z", "browser": "Safari"}
```

5. Check the content of the topic:
```
docker exec -ti wfc_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic browser-stats-stateless --from-beginning

{"browser":"Chrome","window":{"start":"2024-10-06T10:00:00.000.000Z","end":"2024-10-06T10:05:00.000.000Z"},"count":1,
"last_event_time_in_window":"2024-10-06T10:01:00.000.000Z"}
{"browser":"Safari","window":{"start":"2024-10-06T10:00:00.000.000Z","end":"2024-10-06T10:05:00.000.000Z"},"count":1,
"last_event_time_in_window":"2024-10-06T10:02:00.000.000Z"}
{"browser":"Firefox","window":{"start":"2024-10-06T10:00:00.000.000Z","end":"2024-10-06T10:05:00.000.000Z"},"count":1,
"last_event_time_in_window":"2024-10-06T10:00:00.000.000Z"}
```

6. Go back to the producer and generate new visits:
```
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:00:00Z", "browser": "Firefox"}
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:03:00Z", "browser": "Firefox"}
```

Check the results in the consumer's tab:
```
{"browser":"Firefox","window":{"start":"2024-10-06T10:00:00.000.000Z","end":"2024-10-06T10:05:00.000.000Z"},"count":2,
"last_event_time_in_window":"2024-10-06T10:03:00.000.000Z"}
```

As you can notice, the Firefox browser didn't integrate the previous count, thus the aggregation scope is limited to the 
current micro-batch.

7. Stop the job and the Docker containers.

8. Let's do the same test with the stateful job. Start Docker containers. 
9. Start `browsers_stats_generation_stateful_job.py`

10. Open the Kafka producer:
```
docker exec -ti wfc_kafka kafka-console-producer.sh --broker-list localhost:9092 --topic visits
```

11. Open the Kafka consumer:
```
docker exec -ti wfc_kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic browser-stats-stateful --from-beginning
```

12. Generate the first 3 visits and see the outcome in the consumer's console:
```
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:00:00Z", "browser": "Firefox"}
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:01:00Z", "browser": "Chrome"}
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:02:00Z", "browser": "Safari"}
```

It should generate:
```
{"browser":"Firefox","window":{"start":"2024-10-06T10:00:00.000.000Z","end":"2024-10-06T10:05:00.000.000Z"},"count":1,
"last_event_time_in_window":"2024-10-06T10:00:00.000.000Z"}
{"browser":"Chrome","window":{"start":"2024-10-06T10:00:00.000.000Z","end":"2024-10-06T10:05:00.000.000Z"},"count":1,
"last_event_time_in_window":"2024-10-06T10:01:00.000.000Z"}
{"browser":"Safari","window":{"start":"2024-10-06T10:00:00.000.000Z","end":"2024-10-06T10:05:00.000.000Z"},"count":1,
"last_event_time_in_window":"2024-10-06T10:02:00.000.000Z"}
```

13. Add 2 more visits:
```
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:00:00Z", "browser": "Firefox"}
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:03:00Z", "browser": "Firefox"}
```

On the consumer's side you should see that there are already a difference. Previously counted visits for Firefox 
were combined with the 2 new visit records:

```
{"browser":"Firefox","window":{"start":"2024-10-06T10:00:00.000.000Z","end":"2024-10-06T10:05:00.000.000Z"},"count":3,
"last_event_time_in_window":"2024-10-06T10:03:00.000.000Z"}
```

14. Let's introduce now the concept of watermark that you will discover better in the Apache Flink's part. All you need
to know for our scope is that watermark defines the oldest window that can be integrated by the stateful job. Otherwise,
the job would require more hardware resources to run as it computes time-based windows and as you certainly know, time never
goes back. For our case, we accept data up to 20 minutes late (`withWatermark('eventTime', '20 minutes')`). 

To see this in action, let's emit an old window now but within the watermark. 09:40 is an old event time but it's not older
that the accepted event time, which is exactly the same:

```
# producer
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T09:40:00Z", "browser": "Firefox"}
```


The consumer should open a new window:
```
{"browser":"Firefox","window":{"start":"2024-10-06T09:40:00.000Z","end":"2024-10-06T09:45:00.000Z"},"count":1,"last_event_time_in_window":"2024-10-06T09:40:00.000Z"}
```

15. Let's emit now an old event time that happened before the watermark:
```
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T09:30:00Z", "browser": "Firefox"}
```

Normally, the consumer shouldn't emit any new aggregation.

16. Let's advance now the watermark. After sending this event, the new watermark should be 10:10. It should result in
invalidating all pending windows prior to this time:

```
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:30:00Z", "browser": "Firefox"}
```

The consumer should emit a new aggregate:
```
{"browser":"Firefox","window":{"start":"2024-10-06T10:30:00.000Z","end":"2024-10-06T10:35:00.000Z"},"count":1,"last_event_time_in_window":"2024-10-06T10:30:00.000Z"}
```

17. To prove you that the watermark moved on, let's emit now a record for 10:00. Remember from the previous steps, it 
had an active window back then:

```
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:00:00Z", "browser": "Firefox"}
```

However, since the watermark advanced to 10:10, the window for 10:00 is not there anymore.