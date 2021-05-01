from kafka import KafkaProducer
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
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
        # Producer produces data for consumer
        # Data comes from Twitter

        # Parse Data into JSON
        # data_json = json.loads(data.encode('utf-8'))

        # Pretty print tweet on console
        # print(json.dumps(data_json['text'], indent=4, sort_keys=True))
        # print(json.dumps(data_json['entities']['hashtags'], indent=4, sort_keys=True))

        topics = determinate_topic(data)

        # Send data to topic
        for topic in topics:
            self.producer.send("topic_" + topic, data.encode('utf-8'))

        handle_metrics(self.producer.metrics(), topics)
        return True

    def on_error(self, status):
        print(status)
        return True


def determinate_topic(data):
    topics = []
    for keyword in keywords:
        if keyword in data.lower():
            topics.append(keyword)
    return topics


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

    # Get topic from console input
    producer_keywords = input(
        "Enter Keywords to Track in Twitter API - those Keywords are the created topics (seperated by ','): ")
    keywords = producer_keywords.lower().replace(" ", "").split(",")

    # TWITTER API AUTH
    auth = OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    # Twitter Stream Config
    twitter_stream = Stream(auth, KafkaPushListener())

    # Filter the Twitter stream
    twitter_stream.filter(track=keywords)
