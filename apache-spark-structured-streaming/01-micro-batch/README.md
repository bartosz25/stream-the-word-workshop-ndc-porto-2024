# Apache Spark Structured Streaming - micro-batch model

1. Explain the [kafka_consumer_micro_batch.py](kafka_consumer_micro_batch.py)
* this is the most basic Apache Spark Structured Streaming job that includes:
  * checkpoint location; you're going to see how does it work when the job stops
  * trigger that is going to coordinate processing
  * `maxOffsetsPerTrigger` that can be considered as a sign for the micro-batch paradigm; we explicitly ask Apache Spark
  to limit the processing scope, thus to take many rows at once, a little bit like you would do with batch

The code uses `foreachBatch` sink for printing because we want to get the task id. Eventually, you could also use the `.writeStream.format("console")`.

2. Start the Docker images with the visits data generator:
```
cd docker
docker-compose down --volumes; docker-compose up
```

3. Start the `kafka_consumer_micro_batch.py`. You should see some of the visits printed every 20 seconds:
```
✍️ Printing visits
Partition[1] > Printing visit=Row(visit_id='140444734049152_4', event_time=datetime.datetime(2024, 1, 1, 1, 0), page='main')
Partition[1] > Printing visit=Row(visit_id='140444734049152_5', event_time=datetime.datetime(2024, 1, 1, 1, 0), page='about')
Partition[1] > Printing visit=Row(visit_id='140444734049152_6', event_time=datetime.datetime(2024, 1, 1, 1, 0), page='about')
Partition[1] > Printing visit=Row(visit_id='140444734049152_7', event_time=datetime.datetime(2024, 1, 1, 1, 0), page='categories')
Partition[0] > Printing visit=Row(visit_id='140444734049152_0', event_time=datetime.datetime(2024, 1, 1, 1, 0), page='home')
Partition[0] > Printing visit=Row(visit_id='140444734049152_1', event_time=datetime.datetime(2024, 1, 1, 1, 0), page='about')
Partition[0] > Printing visit=Row(visit_id='140444734049152_2', event_time=datetime.datetime(2024, 1, 1, 1, 0), page='contact')
Partition[0] > Printing visit=Row(visit_id='140444734049152_3', event_time=datetime.datetime(2024, 1, 1, 1, 0), page='category_19')
Partition[0] > Printing visit=Row(visit_id='140444734049152_8', event_time=datetime.datetime(2024, 1, 1, 1, 0), page='home')
Partition[0] > Printing visit=Row(visit_id='140444734049152_9', event_time=datetime.datetime(2024, 1, 1, 1, 0), page='about')
✍️ Printing visits

Partition[0] > Printing visit=Row(visit_id='140444734049152_0', event_time=datetime.datetime(2024, 1, 1, 1, 2), page='about')
Partition[0] > Printing visit=Row(visit_id='140444734049152_1', event_time=datetime.datetime(2024, 1, 1, 1, 1), page='index')
Partition[0] > Printing visit=Row(visit_id='140444734049152_2', event_time=datetime.datetime(2024, 1, 1, 1, 1), page='about')
Partition[1] > Printing visit=Row(visit_id='140444734049152_4', event_time=datetime.datetime(2024, 1, 1, 1, 4), page='page_7')
Partition[0] > Printing visit=Row(visit_id='140444734049152_3', event_time=datetime.datetime(2024, 1, 1, 1, 2), page='index')
Partition[0] > Printing visit=Row(visit_id='140444734049152_8', event_time=datetime.datetime(2024, 1, 1, 1, 1), page='main')
Partition[0] > Printing visit=Row(visit_id='140444734049152_9', event_time=datetime.datetime(2024, 1, 1, 1, 5), page='home')
Partition[1] > Printing visit=Row(visit_id='140444734049152_5', event_time=datetime.datetime(2024, 1, 1, 1, 5), page='about')
Partition[1] > Printing visit=Row(visit_id='140444734049152_6', event_time=datetime.datetime(2024, 1, 1, 1, 1), page='index')
Partition[1] > Printing visit=Row(visit_id='140444734049152_7', event_time=datetime.datetime(2024, 1, 1, 1, 4), page='category_15')
```

4. Stop the job and save some of the visits in a text editor:
```
Partition[1] > Printing visit=Row(visit_id='140444734049152_4', event_time=datetime.datetime(2024, 1, 1, 1, 9), page='category_5')
Partition[1] > Printing visit=Row(visit_id='140444734049152_5', event_time=datetime.datetime(2024, 1, 1, 1, 12), page='home')
Partition[1] > Printing visit=Row(visit_id='140444734049152_6', event_time=datetime.datetime(2024, 1, 1, 1, 9), page='index')
```

5. Restart the job and look for the messages you saved before. You shouldn't see them and that's the capability 
the checkpoint provides, i.e. restarting from the last <u>successfully</u> processed place. 

<u>Successfully</u> is an important condition because if your job fails in the middle of a micro-batch, the whole micro-batch
will restart.
