from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic, NewPartitions
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
import re
from tweepy.streaming import StreamListener
from prometheus_client import start_http_server, Counter, Gauge

# TWITTER API CONFIGURATIONS
access_token = "1380977247067209728-zjUF158nYIUPQVzMgTLwBJvHUpOwTW"
access_token_secret = "toLJYQRur9fU3va7eNrLrSw6xY3t9JZV7YLAhzby4gGCn"
api_key = "2uKUCZgWqrLZFr0nGC4kw4ven"
api_secret = "Gk17pGWblSkAB4N57abSCY70LB3IXUcenNEJWPadKhMkIGkP2w"

# Prometheus Metrics
tweet_counter = Counter("received_tweets", "Counts all received Tweets of this producer", ['topic'])
compression_rate_avg_gauge = Gauge("compression_rate_avg", "compression")
request_latency_avg_gauge = Gauge("request_latency_avg", "latency")
batch_size_avg_gauge = Gauge("batch_size_avg", "batch_size_avg")
batch_size_max_gauge = Gauge("batch_size_max", "batch_size_max")


# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        # localhost:9092 = Default Kafka Advertisment Host and Port Adress
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    def on_data(self, data):

        send_topics = determinate_topic(data)

        # Send data to topic
        for topic in send_topics:
            self.producer.send("topic_" + topic, data.encode('utf-8'))

        handle_metrics(self.producer.metrics(), topics)
        return True

    def on_error(self, status):
        print(status)
        return True


def determinate_topic(data):
    send_topics = []
    for topic in topics:
        if topic in data.lower():
            send_topics.append(topic)
    return send_topics


def handle_metrics(metrics, topics):
    for topic in topics:
        tweet_counter.labels(topic).inc()
    compression_rate_avg_gauge.set(metrics["producer-metrics"]["compression-rate-avg"])
    request_latency_avg_gauge.set(metrics["producer-metrics"]["request-latency-avg"])
    batch_size_avg_gauge.set(metrics["producer-metrics"]["batch-size-avg"])
    batch_size_max_gauge.set(metrics["producer-metrics"]["batch-size-max"])


if __name__ == '__main__':

    # Startup Metrics Endpoint
    start_http_server(8000)
    kafka_admin = KafkaAdminClient(bootstrap_servers=['localhost:9092'])

    # Get topic from console input
    producer_keywords = input("Enter Keywords to Track in Twitter API - those Keywords are the created topics (seperated by ','): ")
    topics = re.sub("[^a-zA-Z,1-9]+", "", str(producer_keywords)).lower().split(",")
    keywords = producer_keywords.lower().replace(" ", "").split(",")


    #Input number of partitionsand replicas
    num_partitions = input("Put in the number of partitions per topic:")
    num_replica = input("Put in the number of replicas per topic:")

    print('search for Keywords: ', keywords)
    topic_list = []
    topic_partitions = {}
    topics_existing = kafka_admin.list_topics()
    for topic in topics:
        if "topic_" + topic not in topics_existing:
            topic_list.append(NewTopic(name="topic" + topic, num_partitions=int(num_partitions), replication_factor=int(num_replica)))
        else: #only works for increasing partitions.
            partitions = NewPartitions(total_count=int(num_partitions))
            topic_partitions["topic_"+topic] = partitions
    kafka_admin.create_topics(new_topics=topic_list, validate_only=False)
    kafka_admin.create_partitions(topic_partitions)
    # TWITTER API AUTH
    auth = OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    # Twitter Stream Config
    twitter_stream = Stream(auth, KafkaPushListener())

    # Filter the Twitter stream
    twitter_stream.filter(track=keywords)
