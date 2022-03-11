from tweepy import StreamListener, OAuthHandler, Stream, API
import twitter_cred
import json
import socket
from translate import Translator

my_auth = tweepy.OAuthHandler(twitter_cred.api_key, twitter_cred.api_key_secret)
my_auth.set_access_token(twitter_cred.access_token, twitter_cred.access_token_secret)

class TweetListener(StreamListener):
    def __init__(self, csocket):
        self.client_socket=csocket

    def on_data(self, tweet_data):
        try:
            data = json.loads(tweet_data)
            d={}

            translator=Translator(to_lang = "en")
            d['text'] = translator.translate(data['text'])

            d['created_at'] = data['created_at']
            d['tweet_url'] = 'https://twitter.com/twitter/statuses/' + data['id_str']
            d['user_name'] = data['user']['screen_name']

            if 'extended_entities' in data.keys():
                if 'media' in data['extended_entities']:
                    for media_list in data['extended_entities']['media']:
                        if media_list['type'] == 'photo':
                            d['image_url'] = media_list['media_url_https']
                            print('Image link found : ', d['image_url' ], end='\n------------\n')
                        elif media_list['type'] == 'video':
                            s = media_list['video_info']['variants'][0]['url' ].split("?")[0]
                            if "mp4" in str(s):
                                d['video_url'] = s
                                print('Video link found : ', d['video_url' ], end=' \n------------\n')
                        d=json.dumps(d)
                        self.client_socket.send((str(d)+ "\n").encode('utf-8'))
        except BaseException as e:
            #print("Error in on_data: %s" %str(e))
            return True

    def on_error(self, status):
        print(status)
        return True

TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(5)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
twitter_stream = Stream(my_auth, TweetListener(conn), tweet_mode = 'extended_tweet')
twitter_stream.filter(track=['corona'])