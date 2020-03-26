import json
import requests
import requests_oauthlib



URL = 'https://stream.twitter.com/1.1/statuses/filter.json'

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets(topic):
    query_url = URL + "?track="+topic
    response = requests.get(query_url, auth=my_auth, stream=True)
    
    return response

response = get_tweets("salvini")

for line in response.iter_lines():    
    full_tweets = json.loads(line)
    tweet = full_tweets["text"]
    print(tweet)
    print("-----------------------------------")
    
 
    