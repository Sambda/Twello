from kafka import KafkaConsumer
import json
import threading
import random as rdm
from time import sleep



def create_consumer_group(topic_name, number_of_consumers):
    consumer_group = []
    for i in range(0, number_of_consumers):
        group_id = "group_" + topic_name
        consumer_group.append(consumer_thread(i, topic_name, group_id))
    return consumer_group


def create_consumer(topic_name, group_id):
    return KafkaConsumer(topic_name, auto_offset_reset='earliest', group_id=group_id,
                         bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=10000)


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
        consume(self.consumer)
        print("Exiting: " + self.name)


def consume(cons):
    for msg in cons:
        record = json.loads(msg.value)
        print(json.dumps(record, indent=4, sort_keys=True))
        sleep(rdm.random())

    if cons is not None:
        cons.close()


if __name__ == '__main__':

    # Get topic from console input
    input_topics = input("Enter Topic names (seperated by ','): ")
    topic_names = input_topics.replace(" ", "").split(",")
    topic_names = ["topic_" + topic_name for topic_name in topic_names]
    print(topic_names)

    consumer_per_topic = int(input("Enter a number of consumers per topic (Defines the consumer group size): "))

    consumer_groups = {topic_name: create_consumer_group(topic_name, consumer_per_topic) for topic_name in topic_names}

    # Start Consumer Threads
    for topic in topic_names:
        for consumer_thread in consumer_groups[topic]:
            consumer_thread.start()

    # Wait for al Consumer Threads to finish
    for topic in topic_names:
        counter = 0
        for consumer_thread in consumer_groups[topic]:
            consumer_thread.join()
