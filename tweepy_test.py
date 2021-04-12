from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json

def get_kafka_client():
    return KafkaClient(hosts='127.0.0.1:9092')

class StdOutListener(StreamListener):
    def on_data(self, data):
        print(data)
        message = json.loads(data)
        if message['place'] is not None:
            client = get_kafka_client()
            topic = client.topics['twitterdata2']
            producer = topic.get_sync_producer()
            producer.produce(data.encode('ascii'))
        return True

    def on_error(self, status):
        print(status)

#need tokens and keys

auth = OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_token_secret)
listener = StdOutListener()
stream = Stream(auth, listener)
stream.filter(track=['covid'])
stream.filter(locations=[-180,-90,180,90])


