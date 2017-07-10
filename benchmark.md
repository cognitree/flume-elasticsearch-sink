## Benchmarks
The following is the result of a benchmarking test conducted.

### Setup
#### Hardware specs
 - Make: Apple Macbook Pro
 - CPU: Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz(4 physical cores)
 - RAM: 16GB
 - SSD: 256GB

#### JVM
All processes used the same JVM with default GC settings.
Java version: 1.8.0_101

#### Flume configuration
 - Cluster size: 1
 - Heap size: 512MB
 - Elasticsearch Sink:
    - Max bulk count: 10000
    - Max bulk size: 5 MB
    - Max bulk request interval: 10s

#### Elasticsearch configuration
 - Cluster size: 1
 - Heap size: 2GB
 - Index was pre-created and rest of the configurations were defaults

#### Kafka configuration
 - Cluster size: 1
 - Heap size: 2GB
 - No of partitions: 25

### Test spec
The entire dataset was stored into a Kafka topic before the test. A Flume agent was configured to consume data from this kafka topic and index them into Elasticsearch. The benchmarking metrics below only indicate the times taken to consume from this readily available data on Kafka topic into Elasticsearch.

All the processes (i.e. Flume, Kafka and Elasticsearch) were running on same instance during the test.

#### Test 1: Data is already in JSON format
 - Serializer (in Sink plugin): com.cognitree.flume.sink.elasticsearch.SimpleSerializer
 - Record size: 97.60 bytes
 - Result: It took 15 minutes 19 seconds to index 33,003,944 records into Elasticsearch; No lost events

#### Test 2: Data is available in csv format but needs to be converted to JSON before indexing
 - Serializer (in Sink plugin): com.cognitree.flume.sink.elasticsearch.CsvSerializer
 - Record size: 45.54 bytes
 - Result: It took 15 minutes 47 seconds to index 33,003,944 records into Elasticsearch; No lost events
