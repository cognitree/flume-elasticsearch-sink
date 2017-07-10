#### Overview
 Flume process took the data from kafka topic and stored that inside elastic search configured index.
All the processes(i.e. Flume, Kafka and Elastic search) were running on same hardware specs mentioned.

#### Hardware specs
CPU: Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz(4 physical cores)
Memory: 16GB

#### Flume configuration
Java version: 1.8.0_101       
Java heap size: 512MB

#### Elastic search configuration
Java version: 1.8.0_101    
Java heap size: 2GB

#### Kafka configuration
Java version: 1.8.0_101    
Java heap size: 2GB       
Number of partitions: 25

#### Summary analysis for csv data
Record size: 45.54 bytes.  
Duration: The load test ran for 15 minutes 47 seconds.      
Result: Total events sent: 33,003,944; No lost events.    
Serializer: com.cognitree.flume.sink.elasticsearch.CsvSerializer

#### Summary analysis for json data
Record size: 97.60 bytes.    
Duration: The load test ran for 15 minutes 19 seconds.   
Result: Total events sent: 33,003,944; No lost events.   
Serializer: com.cognitree.flume.sink.elasticsearch.SimpleSerializer
