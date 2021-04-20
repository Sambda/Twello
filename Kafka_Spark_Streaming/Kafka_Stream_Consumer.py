import json
from time import sleep

from kafka import KafkaConsumer

# Get topic from console input
parsed_topic_name = input("Enter Topic name : ")

if __name__ == '__main__':
    #Change Topic name, If not over console input
    #parsed_topic_name = 'twitter_stream_covid'
    followers_count_threshold = 200

    consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    for msg in consumer:
        record = json.loads(msg.value)
        # Some examples for reading keys from twitter
        id_user = int(record['id'])
        name = record['user']['name']
        followers_count = int(record['user']['followers_count'])
        text = record['text']

        if followers_count > followers_count_threshold:
            print('followers_count: {}, name: {}'.format(followers_count, name))
        # delete sleep if not just testing
        sleep(1)

    if consumer is not None:
        consumer.close()
