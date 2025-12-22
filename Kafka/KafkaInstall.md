# Kafka Zookeeper Install

## 1. Install Kafka in Ubuntu

```wget https://archive.apache.org/dist/kafka/3.7.1/kafka_2.13-3.7.1.tgz```

## 2. Untar the Kafka archive, and cd to the kafka directory:

``` Bash
tar -xzf kafka_2.13-3.7.1.tgz 
cd kafka_2.13-3.7.1
```
Run the ls -al command to list the contents of the kafka directory


## 4. Running ZooKeeper

Run the following command to start ZooKeeper:

```cd  kafka_2.13-3.7.1```

```bin/zookeeper-server-start.sh config/zookeeper.properties```

ZooKeeper will be ready in a short time, typically around a second or two.

Open another terminal session. Change the directory to the kafka directory, and start the Kafka broker:

```Bash
cd kafka_2.13-3.7.1
bin/kafka-server-start.sh config/server.properties
```

Open another terminal session and run the kafka-topics command to create a Kafka topic named quickstart-events:

```Bash
cd kafka_2.13-3.7.1
bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092
```

Start Producer

```bin/kafka-console-producer.sh --broker-list localhost:9092 --topic quickstart-events```

Example: 

"jdel@LAPTOP-F85RETIP:~/ kafka_2.13-3.7.1$ bin/kafka-console-producer.sh --broker-//list localhost:9092 --topic quickstart-events" 

Open another terminal session and Start Consumer

```Bash
cd kafka_2.13-3.7.1
bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092
```

In the producer terminal, type a few messages, and watch as they appear in the consumer terminal.


## Stop Kafka

1.	Stop the consumer and producer clients with Ctrl+C
2.	Stop the Kafka broker with Ctrl+C
3.	Stop the ZooKeeper server with Ctrl+C
4.	Run the following command to clean up:
rm -rf /tmp/kafka-logs /tmp/zookeeper

Sources:
https://kafka.apache.org/quickstart

Additional Intro:
https://youtu.be/QkdkLdMBuL0?si=CNS0jmARdSsNDOmH
