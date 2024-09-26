# Apache Spark Structured Streaming - triggers

1. Create the input directory
```
mkdir -p /tmp/ndc-2024/apache-spark-structured-streaming/02-triggers/input
```

2. Add first 8 files:
```
echo "1" >> /tmp/ndc-2024/apache-spark-structured-streaming/02-triggers/input/file1.txt
echo "2" >> /tmp/ndc-2024/apache-spark-structured-streaming/02-triggers/input/file2.txt
echo "3" >> /tmp/ndc-2024/apache-spark-structured-streaming/02-triggers/input/file3.txt
echo "4" >> /tmp/ndc-2024/apache-spark-structured-streaming/02-triggers/input/file4.txt
echo "5" >> /tmp/ndc-2024/apache-spark-structured-streaming/02-triggers/input/file5.txt
echo "6" >> /tmp/ndc-2024/apache-spark-structured-streaming/02-triggers/input/file6.txt
echo "7" >> /tmp/ndc-2024/apache-spark-structured-streaming/02-triggers/input/file7.txt
echo "8" >> /tmp/ndc-2024/apache-spark-structured-streaming/02-triggers/input/file8.txt
```

3. Let's see all the triggers we're going to explore in this demo:

* [processing_time_trigger_job.py](processing_time_trigger_job.py) - the same as you saw in the previous demo, i.e.
the micro-batch will be scheduled at the defined time expression
* [no_trigger_job.py](no_trigger_job.py) - here, Apache Spark will start the next micro-batch
as soon as possible, i.e. after completing the currently running micro-batch
* [available_now_trigger_job.py](available_now_trigger_job.py) - in this configuration Apache Spark will process
all data available at the given moment with the respect of the throughput limitations (`option('maxFilesPerTrigger', 3)`);
consequently, it will run multiple micro-batches to process available data
* [once_trigger_job.py](once_trigger_job.py) - here too, Apache Spark will take all the data available but it
will not respect the throughput limitations; consequently, it will run one micro-batch for the existing data

4. Start `once_trigger_job.py`. You should see a single print in the console, and then the job should stop:

```
24/09/07 04:28:52 WARN MicroBatchExecution: The read limit MaxFiles: 3 for FileStreamSource[file:/tmp/ndc-2024/apache-spark-structured-streaming/02-triggers/input] is ignored when Trigger.Once is used.
-------------------------------------------
Batch: 0
-------------------------------------------
+-----+
|value|
+-----+
|    3|
|    4|
|    5|
|    8|
|    2|
|    1|
|    7|
|    6|
+-----+
```

As you can notice, there is also a warning notifying us about the behavior regarding the read limits.

5. Start `available_now_trigger_job.py`. Here the job should process data with the respect of the throughput:

```
-------------------------------------------
Batch: 0
-------------------------------------------
+-----+
|value|
+-----+
|    3|
|    4|
|    5|
+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+-----+
|value|
+-----+
|    8|
|    2|
|    1|
+-----+

-------------------------------------------
Batch: 2
-------------------------------------------
+-----+
|value|
+-----+
|    7|
|    6|
+-----+
```

6. Start `processing_time_trigger_job.py`. Here you can see that the micro-batches were
running every 30 seconds:

```
-------------------------------------------
Batch: 0
-------------------------------------------
+-----+-----------------------+
|value|processing_time        |
+-----+-----------------------+
|3    |2024-09-07 04:35:00.438|
|4    |2024-09-07 04:35:00.438|
+-----+-----------------------+

-------------------------------------------
Batch: 1
-------------------------------------------
+-----+-----------------------+
|value|processing_time        |
+-----+-----------------------+
|5    |2024-09-07 04:35:30.059|
|8    |2024-09-07 04:35:30.059|
+-----+-----------------------+

-------------------------------------------
Batch: 2
-------------------------------------------
+-----+-----------------------+
|value|processing_time        |
+-----+-----------------------+
|2    |2024-09-07 04:36:00.025|
|1    |2024-09-07 04:36:00.025|
+-----+-----------------------+

-------------------------------------------
Batch: 3
-------------------------------------------
+-----+-----------------------+
|value|processing_time        |
+-----+-----------------------+
|7    |2024-09-07 04:36:30.044|
|6    |2024-09-07 04:36:30.044|
+-----+-----------------------+
```

7. Start `no_trigger_job.py`. It should process files as soon as possible, without the 30 seconds delay:

```
-------------------------------------------
Batch: 0
-------------------------------------------
+-----+-----------------------+
|value|processing_time        |
+-----+-----------------------+
|3    |2024-09-07 04:40:00.114|
|4    |2024-09-07 04:40:00.114|
+-----+-----------------------+

-------------------------------------------
Batch: 1
-------------------------------------------
+-----+-----------------------+
|value|processing_time        |
+-----+-----------------------+
|5    |2024-09-07 04:40:03.353|
|8    |2024-09-07 04:40:03.353|
+-----+-----------------------+

-------------------------------------------
Batch: 2
-------------------------------------------
+-----+-----------------------+
|value|processing_time        |
+-----+-----------------------+
|2    |2024-09-07 04:40:03.592|
|1    |2024-09-07 04:40:03.592|
+-----+-----------------------+

-------------------------------------------
Batch: 3
-------------------------------------------
+-----+-----------------------+
|value|processing_time        |
+-----+-----------------------+
|7    |2024-09-07 04:40:03.838|
|6    |2024-09-07 04:40:03.838|
+-----+-----------------------+
```