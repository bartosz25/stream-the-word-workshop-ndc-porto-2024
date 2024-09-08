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

<iframe width="560" height="315" src="https://www.youtube.com/embed/hxZHxBcVe60?si=SCG25z-myzW3JCxW" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
