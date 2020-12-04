from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from geopy.geocoders import Nominatim
from textblob import TextBlob
from elasticsearch import Elasticsearch
import socket
import re

TCP_IP = 'localhost'
TCP_PORT = 9001

# Get the sentiment of a sentence given the polarity
def getSentiment(polarity):
        if polarity < 0: return "negative"
        if polarity == 0: return "neutral"
        return "positive"

# Initialize Elasticsearch by deleting the indices and adding the geo_point
def initES(hashtag):
        print("Initializing Elasticsearch index " + hashtag)
        mappings = {
                "mappings": {
                        "properties": {
                                "location": {
                                        "type": "geo_point"
                                }
                        }
                }
                
        }

        es = Elasticsearch()

        # Clear out the data from the previous run
        es.indices.delete(index=hashtag, ignore=[400,404])
        es.indices.delete(index="hashtag-*", ignore=[400,404])

        # Create the index using the geo_point settings
        es.indices.create(index=hashtag, body=mappings)

def processTweet(tweet):
    # Here, you should implement:
    # (i) Sentiment analysis,
    # (ii) Get data corresponding to place where the tweet was generate (using geopy or googlemaps)
    # (iii) Index the data using Elastic Search 

    global started

    tweetData = tweet.split("::")

    if len(tweetData) > 2:
        text = tweetData[1]
        rawLocation = tweetData[0]
        hashtag = tweetData[2]

        # (i) Apply Sentiment analysis in "text"
        # Get the polarity to determine sentiment from the tweet
        score = TextBlob(text).sentiment.polarity

	# (ii) Get geolocation (state, country, lat, lon, etc...) from rawLocation

        geolocator = Nominatim(user_agent="spark")
        location = geolocator.geocode(rawLocation)

        # print("\n\nRaw data: " + tweet)
        print("=========================\ntweet: ", str(bytes(text, "utf-8")))
        print("hashtag:", hashtag)
        print("Raw location from tweet status: ", str(bytes(rawLocation, "utf-8")))
        
        #print(location)
        if(location == None): return

        rawLoc = geolocator.reverse([location.latitude, location.longitude])
        if not "country" in rawLoc.raw['address']: return

        country = str(bytes(rawLoc.raw['address']['country'], "utf-8"))[2:-1]

        # Remove special characters
        countryPattern = re.findall(r"^[A-Za-z0-9 _]*[A-Za-z0-9][A-Za-z0-9 _]*", country)
        if len(countryPattern) == 0: return

        country = countryPattern[0].strip()

        print(country)
        print("Coords: " + str(location.latitude) + ", " + str(location.longitude))

        sentiment = getSentiment(score)
        print(sentiment)

        # print("lat: ", lat)
        # print("lon: ", lon)
        # print("state: ", state)
        # print("country: ", country)
        # print("Text: ", text)
        # print("Sentiment: ", sentiment)

        # (iii) Post the index on ElasticSearch or log your data in some other way (you are always free!!) 
        
        json_data = {
                "latitude": location.latitude,
                "longitude": location.longitude,
                "tweet": text,
                "raw_location": rawLocation,
                "location": str(location.latitude) + "," + str(location.longitude),
                "country": country,
                "sentiment": sentiment
        }

        index = hashtag.replace("#","hashtag-").lower()

        for c in [":", ","]:
                index = index.replace(c, "")

        es = Elasticsearch([{'host':'localhost','port':9200}])
        es.index(index=index, body=json_data)


def getHashtagData():
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((TCP_IP, TCP_PORT+1))
        data = sock.recv(1024)
        sock.close()
        
        print("Got data from stream.py")
        print(data)
        print(data.decode("utf-8"))
        return data.decode("utf-8").replace("#", "hashtag-").lower()


hashtagIndex = getHashtagData()
initES(hashtagIndex)

# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# set the log level to one of ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
# sc.sparkContext.setLogLevel("OFF")
sc.setLogLevel("ERROR")

# create the Streaming Context from spark context with interval size 4 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)

dataStream.foreachRDD(lambda rdd: rdd.foreach(processTweet))


ssc.start()
ssc.awaitTermination()
