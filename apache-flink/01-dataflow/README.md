# Apache Flink - Dataflow model
 
1. Explain the [element_based_processing_job.py](element_based_processing_job.py)
* the job processes records from _visits_ topic and classifies each visit regarding its browser
  * if the suffix is "Chrome", it goes to a "chrome" group, otherwise it's partitioned to the "other"
  * visits from the "other" group are processed normally while the "chrome" ones are blocked for 27 hours
    * the blocking is intentional; if the same blocking occurred in the micro-batch model, the job couldn't make any 
    progress, even for the "other" group. Here the dataflow model separates the processing and it will not be an issue.

2. Start the Docker images with the visits data generator:
```
cd docker
docker-compose down --volumes; docker-compose up
```

3. Run `element_based_processing_job.py`. 

4. After running the code, you should see only the "other" browsers printed in the console. Besides, no new "chrome"
records should be ingested after the `key_by` step, as shows the next video:

<video width="800" height="580" controls>
  <source src="assets/flink_blocked_task.mp4" type="video/mp4">
</video>

[assets/flink_blocked_task.mp4](assets/flink_blocked_task.mp4)
