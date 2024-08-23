# Apache Spark Structured Streaming - micro-batch model

1. Start Localstack:
```
LAMBDA_DOCKER_NETWORK=host
localstack start -d
```

2. Create Kinesis Data Streams:
```
awslocal kinesis create-stream --stream-name visits-ordered --shard-count 2
```


```
✍️ Printing visits
Partition[1] > Printing visit=Row(visit_id='140485318146944_4', event_time=datetime.datetime(2024, 1, 1, 2, 53), page='page_19')
Partition[1] > Printing visit=Row(visit_id='140485318146944_5', event_time=datetime.datetime(2024, 1, 1, 3, 5), page='index')
Partition[1] > Printing visit=Row(visit_id='140485318146944_6', event_time=datetime.datetime(2024, 1, 1, 3, 6, 22), page='category_1')
Partition[1] > Printing visit=Row(visit_id='140485318146944_7', event_time=datetime.datetime(2024, 1, 1, 2, 53), page='category_4')
Partition[1] > Printing visit=Row(visit_id='140485318146944_4', event_time=datetime.datetime(2024, 1, 1, 2, 54), page='categories')
Partition[1] > Printing visit=Row(visit_id='140485318146944_5', event_time=datetime.datetime(2024, 1, 1, 3, 7), page='home')
Partition[1] > Printing visit=Row(visit_id='140485318146944_6', event_time=datetime.datetime(2024, 1, 1, 3, 8, 22), page='categories')
Partition[1] > Printing visit=Row(visit_id='140485318146944_7', event_time=datetime.datetime(2024, 1, 1, 2, 55), page='contact')
Partition[1] > Printing visit=Row(visit_id='140485318146944_4', event_time=datetime.datetime(2024, 1, 1, 2, 56), page='main')
Partition[1] > Printing visit=Row(visit_id='140485318146944_5', event_time=datetime.datetime(2024, 1, 1, 3, 12), page='contact')
Partition[1] > Printing visit=Row(visit_id='140485318146944_6', event_time=datetime.datetime(2024, 1, 1, 3, 10, 22), page='page_6')
Partition[1] > Printing visit=Row(visit_id='140485318146944_7', event_time=datetime.datetime(2024, 1, 1, 2, 59), page='index')
Partition[1] > Printing visit=Row(visit_id='140485318146944_4', event_time=datetime.datetime(2024, 1, 1, 2, 58), page='about')
Partition[1] > Printing visit=Row(visit_id='140485318146944_5', event_time=datetime.datetime(2024, 1, 1, 3, 14), page='page_12')
Partition[1] > Printing visit=Row(visit_id='140485318146944_6', event_time=datetime.datetime(2024, 1, 1, 3, 11, 22), page='page_3')
Partition[1] > Printing visit=Row(visit_id='140485318146944_7', event_time=datetime.datetime(2024, 1, 1, 3, 2), page='page_16')

Partition[0] > Printing visit=Row(visit_id='140485318146944_0', event_time=datetime.datetime(2024, 1, 1, 3, 1, 28), page='index')
Partition[0] > Printing visit=Row(visit_id='140485318146944_1', event_time=datetime.datetime(2024, 1, 1, 2, 57), page='index')
Partition[0] > Printing visit=Row(visit_id='140485318146944_2', event_time=datetime.datetime(2024, 1, 1, 2, 58), page='about')
Partition[0] > Printing visit=Row(visit_id='140485318146944_3', event_time=datetime.datetime(2024, 1, 1, 3, 0), page='category_1')
Partition[0] > Printing visit=Row(visit_id='140485318146944_8', event_time=datetime.datetime(2024, 1, 1, 3, 1), page='main')
Partition[0] > Printing visit=Row(visit_id='140485318146944_9', event_time=datetime.datetime(2024, 1, 1, 3, 5), page='contact')
Partition[0] > Printing visit=Row(visit_id='140485318146944_0', event_time=datetime.datetime(2024, 1, 1, 3, 2, 28), page='page_20')
Partition[0] > Printing visit=Row(visit_id='140485318146944_1', event_time=datetime.datetime(2024, 1, 1, 3, 0), page='page_2')
Partition[0] > Printing visit=Row(visit_id='140485318146944_2', event_time=datetime.datetime(2024, 1, 1, 3, 0), page='categories')
Partition[0] > Printing visit=Row(visit_id='140485318146944_3', event_time=datetime.datetime(2024, 1, 1, 3, 2), page='home')
Partition[0] > Printing visit=Row(visit_id='140485318146944_8', event_time=datetime.datetime(2024, 1, 1, 3, 5), page='about')
Partition[0] > Printing visit=Row(visit_id='140485318146944_9', event_time=datetime.datetime(2024, 1, 1, 3, 9), page='home')
Partition[0] > Printing visit=Row(visit_id='140485318146944_0', event_time=datetime.datetime(2024, 1, 1, 3, 7, 28), page='main')
Partition[0] > Printing visit=Row(visit_id='140485318146944_1', event_time=datetime.datetime(2024, 1, 1, 3, 2), page='main')
Partition[0] > Printing visit=Row(visit_id='140485318146944_2', event_time=datetime.datetime(2024, 1, 1, 3, 5), page='page_13')
Partition[0] > Printing visit=Row(visit_id='140485318146944_3', event_time=datetime.datetime(2024, 1, 1, 3, 4), page='about')
Partition[0] > Printing visit=Row(visit_id='140485318146944_8', event_time=datetime.datetime(2024, 1, 1, 3, 6), page='index')
Partition[0] > Printing visit=Row(visit_id='140485318146944_9', event_time=datetime.datetime(2024, 1, 1, 3, 11), page='category_9')
Partition[0] > Printing visit=Row(visit_id='140485318146944_0', event_time=datetime.datetime(2024, 1, 1, 3, 11, 28), page='home')
Partition[0] > Printing visit=Row(visit_id='140485318146944_1', event_time=datetime.datetime(2024, 1, 1, 3, 4), page='page_3')
Partition[0] > Printing visit=Row(visit_id='140485318146944_2', event_time=datetime.datetime(2024, 1, 1, 3, 10), page='page_20')
Partition[0] > Printing visit=Row(visit_id='140485318146944_3', event_time=datetime.datetime(2024, 1, 1, 3, 5), page='categories')
Partition[0] > Printing visit=Row(visit_id='140485318146944_8', event_time=datetime.datetime(2024, 1, 1, 3, 10), page='page_1')
Partition[0] > Printing visit=Row(visit_id='140485318146944_9', event_time=datetime.datetime(2024, 1, 1, 3, 14), page='about')
```