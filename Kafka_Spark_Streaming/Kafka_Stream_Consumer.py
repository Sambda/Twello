from kafka import KafkaConsumer
from prometheus_client import start_http_server, Counter, Gauge
import json
from time import sleep

#consum_counter = Counter("consume_counter", "Counts all received Tweets of this producer")
#compression_rate_avg_gauge = Gauge("compression_rate_avg", "compression")

records_lag_max_gauge = Gauge("records_lag_max", "records_lag_max")
bytes_consumed_rate_gauge = Gauge("bytes_consumed_rate", "bytes_consumed_rate")
fetch_rate_gauge = Gauge("fetch_rate", "fetch_rate")

# Get topic from console input
#parsed_topic_name = input("Enter Topic name : ")
parsed_topic_name = "topic_bigdata"

if __name__ == '__main__':
    start_http_server(8000)
    #Change Topic name, If not over console input
    #followers_count_threshold = 200

    consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)

    #print(consumer.metrics())

    for msg in consumer:
        record = json.loads(msg.value)
        records_lag_max_gauge.set(consumer.metrics()["consumer-fetch-manager-metrics"]["records-lag-max"])
        bytes_consumed_rate_gauge.set(consumer.metrics()["consumer-fetch-manager-metrics"]["bytes-consumed-rate"])
        fetch_rate_gauge.set(consumer.metrics()["consumer-fetch-manager-metrics"]["fetch-rate"])
        sleep(1)
        '''
        # Some examples for reading keys from twitter
        id_user = int(record['id'])
        name = record['user']['name']
        followers_count = int(record['user']['followers_count'])
        text = record['text']

        if followers_count > followers_count_threshold:
            print('followers_count: {}, name: {}'.format(followers_count, name))
        # delete sleep if not just testing
        consum_counter.inc()
        sleep(1)
        '''

    if consumer is not None:
        consumer.close()
