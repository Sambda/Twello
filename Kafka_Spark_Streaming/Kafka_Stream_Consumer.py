from kafka import KafkaConsumer
from prometheus_client import start_http_server, Counter, Gauge
import json
import threading
import random as rdm
from time import sleep

consumer_counter = Counter("consume_counter", "Counts all received Tweets of this consumer-group", ['topic'])
records_lag_max_gauge = Gauge("records_lag_max", "records_lag_max", ['topic'])
bytes_consumed_rate_gauge = Gauge("bytes_consumed_rate", "bytes_consumed_rate", ['topic'])
fetch_rate_gauge = Gauge("fetch_rate", "fetch_rate", ['topic'])
records_consumed_rate_gauge = Gauge("records_consumed_rate", "records_consumed_rate", ['topic'])


def create_consumer_group(topic_name, number_of_consumers):
    consumer_group = []
    for i in range(0, number_of_consumers):
        group_id = "group_" + topic_name
        consumer_group.append(consumer_thread(i, topic_name, group_id))
    return consumer_group


def create_consumer(topic_name, group_id):
    parsed_topic_name = 'topic_' + topic_name
    return KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest', group_id=group_id,
                         bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)


class consumer_thread(threading.Thread):
    def __init__(self, threadID, topic_name, group_id):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.topic = topic_name
        self.group_id = group_id
        self.name = topic_name + "consumer" + str(threadID)
        self.consumer = create_consumer(topic_name, group_id)

    def run(self):
        print("Starting Name: {} Group_id: {}".format(self.name, self.group_id))
        consume(self.consumer, self.topic, self.threadID)
        print("Exiting: " + self.name)


def consume(cons, topic, id):
    for msg in cons:
        print(str(id) + "_consumed_" + topic)
        record = json.loads(msg.value)
        handle_metrics(cons.metrics(), topic)
        sleep(rdm.random())

    if cons is not None:
        cons.close()


def handle_metrics(metrics, topic):
    records_lag_max_gauge.labels(topic).set(metrics["consumer-fetch-manager-metrics"]["records-lag-max"])
    bytes_consumed_rate_gauge.labels(topic).set(metrics["consumer-fetch-manager-metrics"]["bytes-consumed-rate"])
    records_consumed_rate_gauge.labels(topic).set(metrics["consumer-fetch-manager-metrics"]["records-consumed-rate"])
    fetch_rate_gauge.labels(topic).set(metrics["consumer-fetch-manager-metrics"]["fetch-rate"])
    consumer_counter.labels(topic).inc()


if __name__ == '__main__':

    # Get topic from console input
    input_topics = input("Enter Topic names (seperated by ','): ")
    topic_names = input_topics.replace(" ", "").split(",")

    consumer_per_topic = int(input("Enter a number of consumers per topic (Defines the consumer group size: "))

    start_http_server(8001)
    consumer_groups = {topic_name: create_consumer_group(topic_name, consumer_per_topic) for topic_name in topic_names}

    # Start Consumer Threads
    for topic in topic_names:
        counter = 0
        for consumer_thread in consumer_groups[topic]:
            consumer_thread.start()

    # Wait for al Consumer Threads to finish
    for topic in topic_names:
        counter = 0
        for consumer_thread in consumer_groups[topic]:
            consumer_thread.join()
