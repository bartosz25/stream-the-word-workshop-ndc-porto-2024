# Apache Spark Structured Streaming - micro-batch model
 
```
docker exec -ti wfc_kafka kafka-console-producer.sh --broker-list localhost:9092 --topic visits
```

{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:00:00Z", "browser": "Firefox"}
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:01:00Z", "browser": "Chrome"}
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:02:00Z", "browser": "Safari"}

{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:00:00Z", "browser": "Firefox"}
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:03:00Z", "browser": "Firefox"}


// OLD window
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T09:40:00Z", "browser": "Firefox"}

// OLD window but before the watermark
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T09:30:00Z", "browser": "Firefox"}

// Advance the watermark
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:30:00Z", "browser": "Firefox"}

// Too old now compared to the watermark
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:00:00Z", "browser": "Firefox"}

```
docker exec wfc_kafka kafka-console-consumer.sh --topic browser-stats --bootstrap-server localhost:9092 --from-beginning

```