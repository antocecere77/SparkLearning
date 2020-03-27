import socket
import json
import requests
import requests_oauthlib

CONSUMER_KEY = 'TWITTER_API_KEY'
CONSUMER_SECRET = 'WITTER_API SECRET_KEY'
ACCESS_TOKEN = 'TWITTER ACCESS TOKEN'
ACCESS_SECRET = 'TWITTER ACCESS TOKEN SECRET'

CONSUMER_KEY = 'RNv0L0oXWYemfybypHM079UWH'
CONSUMER_SECRET = '7CZUp8Ja127yzACC2JU81qTnj5XB9GsiDx6wuZG3PLoJksHgyQ'
ACCESS_TOKEN = '2919693094-0AGXjCD2V1D3M7KJrCZHC4lpoZVaaMkULm8fz9w'
ACCESS_SECRET = 'cnabOiYmbfyGlz0COMp3l1343qUZDABI7XYldytaU9i6C'

URL = 'https://stream.twitter.com/1.1/statuses/filter.json'

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets(topic):
    query_url = URL + "?track="+topic
    response = requests.get(query_url, auth=my_auth, stream=True)
    
    return response

# response = get_tweets("salvini")

#for line in response.iter_lines():    
#    full_tweets = json.loads(line)
#    tweet = full_tweets["text"]
#    print(tweet)
#    print("-----------------------------------")

def send_tweets(response, conn):
    for line in response.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet = full_tweet["text"]
            print("Tweet:" + tweet)
            print("-----------------------------------")
            conn.send((tweet + "\n").encode())
        except Exception as e:
            print(e)
    
topic = input("Cosa vuoi monitorare? ").lower()

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('localhost', 9999))
s.listen(1)

print("In attesa di una connessione...")

conn, addr = s.accept()

print("Connesso! In attesa di tweets...")

response = get_tweets(topic)
send_tweets(response, conn)



    
 

    