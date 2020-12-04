import tweepy
import socket
import re
import emoji
import html
# import preprocessor
import sys


# Enter your Twitter keys here!!!
ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

hashtag = "#covid19"

# python stream.py [hashtag]
if len(sys.argv) > 1:
    hashtag = sys.argv[1]

TCP_IP = 'localhost'
TCP_PORT = 9001




def preprocessing(tweet):
    
    # Add here your code to preprocess the tweets and  
    # remove Emoji patterns, emoticons, symbols & pictographs, transport & map symbols, flags (iOS), etc

    # Remove links
    cleaned_tweet = re.sub(r"http\S+", "", tweet)

    # Remove emojis
    cleaned_tweet = emoji.get_emoji_regexp().sub(u'', cleaned_tweet)

    # Decode HTML characters, ex: &amp; -> &
    cleaned_tweet = html.unescape(cleaned_tweet)

    print(cleaned_tweet)

    return cleaned_tweet




def getTweet(status):
    
    # You can explore fields/data other than location and the tweet itself. 
    # Check what else you could explore in terms of data inside Status object

    tweet = ""
    location = ""

    location = status.user.location
    
    if hasattr(status, "retweeted_status"):  # Check if Retweet
        try:
            tweet = status.retweeted_status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.retweeted_status.text
    else:
        try:
            tweet = status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.text

    return location, preprocessing(tweet)


# Send the hashtag over to spark.py since it changes each time depending on the parameters
def sendHashtag():
    print("Hashtag server started, waiting for data")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((TCP_IP, TCP_PORT+1))
    sock.listen(1)
    conn, addr = sock.accept()

    conn.send(bytes(hashtag, "utf-8"))
    print("Sent hashtag")

    sock.close()

sendHashtag()

# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        location, tweet = getTweet(status)
        if (location != None and tweet != None):
            tweetLocation = location + "::" + tweet + "::" + hashtag + "\n"
            conn.send(tweetLocation.encode('utf-8'))

        return True


    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener(), tweet_mode="extended")
myStream.filter(track=[hashtag], languages=["en"])


