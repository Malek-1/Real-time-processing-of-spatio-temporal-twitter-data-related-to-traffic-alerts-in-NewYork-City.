#import the necessary packages 
import requests
import os
import csv
from kafka import KafkaProducer
import json
import time 
import re

# Set your "Bearer_Token" key for Twitter Api V2 authorization 
bearer_token = os.environ.get("Put your bearer token here")
producer = KafkaProducer(bootstrap_servers='localhost:9092') 
# Set the name of Kafka topic 
topic_name = "congestion"
# URL to scrape the stream of recent tweets 
search_url = "https://api.twitter.com/2/tweets/search/recent"

# Set the query parameters to filter tweets 
query_params = {'query': 'from:NYC_Alerts911','tweet.fields': 'created_at', 'max_results':'100'}

# helper function for bearer token authentication 
def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    
    r.headers["Authorization"] = f"Bearer {put your bearer token key here }"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r

# helper function to connect to stream 
def connect_to_endpoint(url, params):
    response = requests.get(url, auth=bearer_oauth, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

# main() function 
def main():
    while True:
        json_response = connect_to_endpoint(search_url, query_params)
        tweets = json.loads(json.dumps(json_response, indent=4, sort_keys=True))
        for tw in tweets['data'] : 
            
            producer.send("congestion", json.dumps(tw).encode('utf-8'))
           
            print("Tweets sent to consumer")
        time.sleep(1) 

if __name__ == "__main__":
    main()
