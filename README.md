# Twello

## Start Zookeeper 
1. $ cd <PATH_TO_KAFKA>/kafka_2.13-2.7.0
2. $ bin/zookeeper-server-start.sh config/zookeeper.properties

## Start Kafka
3. $ cd <PATH_TO_KAFKA>/kafka_2.13-2.7.0
4. $ bin/kafka-server-start.sh config/server.properties

## Create Kafka Topic from Twitter and Save Logs 
$ cd <PATH_TO_FILE>
$ python StreamProducer.py (Topic name as console parameter, saves all keys)

## Read Topic and print in Console
$ cd <PATH_TO_KAFKA>/kafka_2.13-2.7.0
$ bin/kafka-console-consumer.sh --topic <TOPIC_NAME> --from-beginning --bootstrap-server localhost:9092

## Consume Kafka Topic
$ cd <PATH_TO_FILE>
$ python Consumer_kafka_topic.py (Topic name as console parameter) 
-> Topic name is topic_+entered_hashtag, if Hashtag was "dog" -> topic name is "topic_dog"
