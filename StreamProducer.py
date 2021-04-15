from kafka import KafkaProducer
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# TWITTER API CONFIGURATIONS
access_token = "1380977247067209728-zjUF158nYIUPQVzMgTLwBJvHUpOwTW"
access_token_secret =  "toLJYQRur9fU3va7eNrLrSw6xY3t9JZV7YLAhzby4gGCn"
api_key =  "2uKUCZgWqrLZFr0nGC4kw4ven"
api_secret =  "Gk17pGWblSkAB4N57abSCY70LB3IXUcenNEJWPadKhMkIGkP2w"

hashtag = input("Enter the hashtag : ")
print("Created Topic name: topic_{}".format(hashtag))
# TWITTER API AUTH
auth = OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)


# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    # Get Producer that has topic name is Twitter
    # self.producer = self.client.topics[bytes("twitter")].get_producer()

    def on_data(self, data):
        # Producer produces data for consumer
        # Data comes from Twitter
        self.producer.send("topic_" + hashtag, data.encode('utf-8'))
        #self.producer.send("twitter_stream_" + hashtag, data.encode('utf-8'))
        parsed = json.loads(data)
        print(json.dumps(parsed, indent=4, sort_keys=True))
        return True

    def on_error(self, status):
        print(status)
        return True


# Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

hashStr = "#"+ hashtag

# Produce Data that has trump hashtag (Tweets)
twitter_stream.filter(track=[hashStr])
