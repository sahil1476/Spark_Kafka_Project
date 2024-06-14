<img src="https://github.com/sahil1476/Spark_Kafka_Project/assets/94641122/7ef660d9-5352-4a1f-81e2-4691c5e75ad8" alt="Sample Image" width="100%" height="400">

## Set Up Kafka `Topic` in which you are going to send and read data from `Spark`

## Thing you need to Know 

# Apache Kafka

Apache Kafka - public - subscribe messaging system
   kafka is Distributed Streaming Platform or Commint log,  used for building real-time data pipelines and streaming applications.
  
#### Some basic terms to understand Kafka:

1. `Topic`
A topic is a category or feed name to which records are published. Kafka topics are always multi-subscriber; a topic can have zero, one, or many consumers that subscribe to the data written to it.

2. `Partition`
Topics in Kafka are split into partitions. Each partition is an ordered, immutable sequence of records that is continually appended to—a structured commit log. Partitions allow Kafka to scale horizontally by distributing data across multiple brokers.

3. `Broker`
A Kafka broker is a server that stores data and serves clients. Each broker hosts one or more partitions and is responsible for handling the read and write requests for the partitions it hosts, as well as for replicating data to other brokers.

4. `Producer`
A producer is a client application that publishes (writes) records to Kafka topics. Producers send data to Kafka brokers, which store the data in the appropriate topic partitions.

5. `Consumer`
A consumer is a client application that subscribes to (reads) records from one or more Kafka topics. Consumers are part of a consumer group, and each consumer in the group processes data from different partitions to achieve parallel processing.

6. `Consumer Group`
A consumer group is a group of consumers that work together to consume data from a topic. Each consumer in the group reads from different partitions to ensure that the data is processed in parallel without duplication.

7. `Offset`
An offset is a unique identifier for each record within a partition. It is a sequential number that Kafka assigns to records as they are produced to the partition. Consumers use offsets to keep track of which records they have processed.

8. `Replication`
Replication in Kafka involves duplicating data from one broker (the leader) to other brokers (followers) to ensure fault tolerance. Each partition has one leader and zero or more followers. The leader handles all reads and writes, while the followers replicate the data.

9. `ZooKeeper`
Apache ZooKeeper is used by Kafka to manage and coordinate the Kafka brokers. It keeps track of the status of Kafka brokers and topics, as well as manages access control. ZooKeeper helps Kafka maintain cluster state and leader election.

10. `Kafka Connect`
Kafka Connect is a tool for scalably and reliably streaming data between Apache Kafka and other systems. It includes connectors to various data sources and sinks, allowing Kafka to integrate with databases, key-value stores, search indexes, and file systems.

11. `Kafka Streams`
Kafka Streams is a library for building applications and microservices, where the input and output data are stored in Kafka clusters. It allows for the processing of real-time streams of data, providing capabilities for stateless and stateful operations, windowing, and joins.

12. `Leader and Follower`
In the context of replication, each partition has one broker that acts as the leader and one or more brokers that act as followers. The leader handles all read and write requests, while followers replicate the leader’s data to ensure redundancy.

13. `Log`
A log in Kafka is a topic partition. It is an append-only sequence of records that Kafka brokers store. Each log is identified by a topic and a partition number.

14. `Retention`
Retention in Kafka refers to how long Kafka retains records in a topic before they are eligible for deletion. Retention can be based on time (e.g., retain records for seven days) or on size (e.g., retain up to 1 GB of data).


```
# Kafka combines three key capabilities so you can implement your use cases for event streaming
  end-to-end with a single battle-tested solution:

1. To publish (write) and subscribe to (read) streams of events, including continuous
   import/export of your data from other systems.
2. To store streams of events durably and reliably for as long as you want.
3. To process streams of events as they occur or retrospectively.
```



## How to START THE KAFKA ENVIRONMENT
   - You need to set up Kafka [For additional Details see here](https://kafka.apache.org/documentation/)

### Kafka with ZooKeeper

Run the following commands in order to start all services in the correct order:

### Start the ZooKeeper service
```$ bin/zookeeper-server-start.sh config/zookeeper.properties```
Open another terminal session and run:

### Start the Kafka broker service
```$ bin/kafka-server-start.sh config/server.properties```
Once all services have successfully launched, you will have a basic Kafka environment running and ready to use.

### Kafka with KRaft

Kafka can be run using KRaft mode using local scripts and downloaded files or the docker image. Follow one of the sections below but not both to start the kafka server.

#### Using downloaded files

Generate a Cluster UUID

```$ KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"```
Format Log Directories

```$ bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties```
Start the Kafka Server

### Using docker image

```$ bin/kafka-server-start.sh config/kraft/server.properties```
Get the docker image

```$ docker pull apache/kafka:3.7.0```
Start the kafka docker container

```$ docker run -p 9092:9092 apache/kafka:3.7.0```
Once the Kafka server has successfully launched, you will have a basic Kafka environment running and ready to use.

## CREATE A TOPIC TO STORE YOUR EVENTS
```
$ bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092
```
All of Kafka's command line tools have additional options: run the kafka-topics.sh command without any arguments to display usage information. For example, it can also show you details such as the partition count of the new topic:

```$ bin/kafka-topics.sh --describe --topic quickstart-events --bootstrap-server localhost:9092
Topic: quickstart-events        TopicId: NPmZHyhbR9y00wMglMH2sg PartitionCount: 1       ReplicationFactor: 1	Configs:
    Topic: quickstart-events Partition: 0    Leader: 0   Replicas: 0 Isr: 0
 ```
## WRITE SOME EVENTS INTO THE TOPIC
```$ bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
This is my first event
This is my second event
```
You can stop the producer client with Ctrl-C at any time.

## READ THE EVENTS
```
$ bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092
This is my first event
This is my second event
```
##### This is all you need to work with the project

Now install `Spark`. 
# Apache Spark 
   an open-source, distributed computing system designed for fast and general-purpose cluster computing. It extends the MapReduce model to efficiently support more types of computations, 
   including interactive queries and stream processing.
#### Languages Supported by Apache Spark 
      Scala
      Java
      Python - Pyspark ( we are using this )
# My Document

This is a button that opens the Apache Spark PySpark documentation:

<button class="styled-button" onclick="window.location.href='https://spark.apache.org/docs/latest/api/python/getting_started/index.html'">Go to PySpark Documentation</button>



Go and check the code now at [check here](https://github.com/sahil1476/Spark_Kafka_Project/tree/main/taskimp ).


