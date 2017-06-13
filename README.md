**Elasticsearch Sink**

The sink reads events from a channel, serializes them into json documents and batches them into a bulk processor.
Bulk processor batches the writes to elasticsearch as configured.

The elasticsearch index, type and id for each event can be defined statically in the configuration file or can be derived dynamically using a custom IndexBuilder. By default it will use the index name from the configuration file.

By default, events are assumed to be in json format.
This assumption can be overridden by implementing the Serializer interface.
 
You need to Follow this Steps to use this sink in Apache flume.
Execute the following command in project.

mvn clean assembly:assembly

This command will create the zip file inside the target folder of the project.
Unzip the file and put it inside the flume installation directories plugin.d folder.
Now you can use the this sink by providing the required properties inside 
flume-conf.properties file of apache flume Installation.

Required properties are in bold.

| Property Name                              | Default      | Description                                                                                   |
|--------------------------------------------|:--------------:|:-----------------------------------------------------------------------------------------------|
| **channel**                                    | -            |                                                                                               |
| **type**                                       | -            | The component type name, needs to be com.cognitree.flume.sink.elasticsearch.ElasticSearchSink |
| **es.cluster.name**                            | -            | Name of the elastic search cluster to connect to                                              |
| **es.client.hostName**                         | -            | Hostname for the elastic search node                                                          |
| **es.client.port**                             | -            | Port for the elastic search hostname                                                          |
| es.bulkActions                             | 1000         | Execute the bulk every mentioned requests                                                     |
| es.bulkProcessor.name                      | -            | Name of the bulk processor                                                                    |
| es.bulkSize                                | 5            | Flush the bulk request every mentioned size                                                   |
| es.bulkSize.unit                           | MB           | Bulk request unit                                                                             |
| es.concurrent.request                      | 1            |  Concurrent request is allowed to be executed while accumulating new bulk requests.           |
| es.flush.interval.time                     | -            | Flush the bulk request every mentioned seconds whatever the number of requests                |
| es.backoff.policy.time.interval            | 50ms         | Backoff policy time interval, wait initially for the 50 mili seconds                          |
| es.backoff.policy.retries                  | 8            | Backoff policy retries                                                                        |
| es.client.transport.sniff                  | false        | To enable or disable the sniff feature of the elastic search                                  |
| es.client.transport.ignore_cluster_name    | false        | To ignore cluster name validation of connected nodes                                          |
| es.client.transport.ping_timeout           | 5s           | The time to wait for a ping response from a node                                              |
| es.client.transport.nodes_sampler_interval | 5s           | How often to sample / ping the nodes listed and connected                                     |
| es.index.name                              | defaultindex | Index name to be used to store the documents                                                  |
| es.index.type                              | defaulttype  | Index type to be used to store the documents                                                  |
| es.index.builder                           |com.cognitree.flume.sink.elasticsearch.SimpleIndexBuilder            | Implementation of com.cognitree.flume.sink.elasticsearch.IndexBuilder interface accepted      |
| es.serializer.builder                      |com.cognitree.flume.sink.elasticsearch.SimpleSerializer            | Implementation of com.cognitree.flume.sink.elasticsearch.Serializer interface accepted |


Example of agent named a1
````
  a1.channels = c1
  a1.sinks = k1
  a1.sinks.k1.type=com.cognitree.flume.sink.elasticsearch.ElasticSearchSink
  a1.sinks.k1.es.bulkActions=5
  a1.sinks.k1.es.bulkProcessor.name=bulkprocessor
  a1.sinks.k1.es.bulkSize=5
  a1.sinks.k1.es.bulkSize.unit=MB
  a1.sinks.k1.es.concurrent.request=1
  a1.sinks.k1.es.flush.interval.time=5m
  a1.sinks.k1.es.backoff.policy.time.interval=50M
  a1.sinks.k1.es.backoff.policy.retries=8
  a1.sinks.k1.es.cluster.name=es-cluster
  a1.sinks.k1.es.client.transport.sniff=false
  a1.sinks.k1.es.client.transport.ignore_cluster_name=false
  a1.sinks.k1.es.client.transport.ping_timeout=5s
  a1.sinks.k1.es.client.transport.nodes_sampler_interval=5s
  a1.sinks.k1.es.client.hostName=localhost
  a1.sinks.k1.es.client.port=9300
  a1.sinks.k1.es.index.name=defaultindex
  a1.sinks.k1.es.index.type=defaulttype
  a1.sinks.k1.es.index.builder=com.cognitree.flume.sink.elasticsearch.HeaderBasedIndexBuilder
  a1.sinks.k1.es.serializer.builder=com.cognitree.flume.sink.elasticsearch.SimpleSerializer
````