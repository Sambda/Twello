from kafka import KafkaProducer
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from prometheus_client import start_http_server, Counter

# Console Input for the wanted hashtag
hashtag = input("Enter the hashtag : ")
print("Created Topic name: topic_{}".format(hashtag))

# TWITTER API CONFIGURATIONS
access_token = "1380977247067209728-zjUF158nYIUPQVzMgTLwBJvHUpOwTW"
access_token_secret =  "toLJYQRur9fU3va7eNrLrSw6xY3t9JZV7YLAhzby4gGCn"
api_key =  "2uKUCZgWqrLZFr0nGC4kw4ven"
api_secret =  "Gk17pGWblSkAB4N57abSCY70LB3IXUcenNEJWPadKhMkIGkP2w"

# TWITTER API AUTH
auth = OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

#Prometheus
tweet_counter = Counter("received_tweets", "Counts all received Tweets of this producer")

# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        start_http_server(8000)
    # Get Producer that has topic name is Twitter
    # self.producer = self.client.topics[bytes("twitter")].get_producer()

    def on_data(self, data):
        # Producer produces data for consumer
        # Data comes from Twitter
        self.producer.send("topic_" + hashtag, data.encode('utf-8'))

        #Pretty print tweet on console
        #parsed = json.loads(data)
        #print(json.dumps(parsed, indent=4, sort_keys=True))

        #Metric Stuff
        tweet_counter.inc()
        print("Compression Rate: ", self.producer.metrics()["producer-metrics"]["compression-rate-avg"])
        print("Response Rate: ", self.producer.metrics()["producer-metrics"]["response-rate"])
        print("Request Rate: ", self.producer.metrics()["producer-metrics"]["request-rate"])
        print("request-latency-avg: ", self.producer.metrics()["producer-metrics"]["request-latency-avg"])
        print("io-wait-time-ns-avg: ", self.producer.metrics()["producer-metrics"]["io-wait-time-ns-avg"])
        print("batch size avg: ", self.producer.metrics()["producer-metrics"]["batch-size-avg"])

        return True

    def on_error(self, status):
        print(status)
        return True


# Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

hashStr = "#"+ hashtag

# Produce Data that has trump hashtag (Tweets)
twitter_stream.filter(track=[hashStr])
