import argparse
import json
import oauth2 as oauth
import os
import sys
import urllib2 as urllib

parser = argparse.ArgumentParser(description='Fetch tweets')
parser.add_argument('--max_tweets', action="store", dest="max_tweets", type=int, default=0)
parser.add_argument('--location', action="store", dest="location", default=None)

access_token_key = os.environ['ACCESS_TOKEN_KEY']
access_token_secret = os.environ['ACCESS_TOKEN_SECRET']

consumer_key = os.environ['CONSUMER_KEY']
consumer_secret = os.environ['CONSUMER_SECRET']

_debug = 0

oauth_token    = oauth.Token(key=access_token_key, secret=access_token_secret)
oauth_consumer = oauth.Consumer(key=consumer_key, secret=consumer_secret)

signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()

http_method = "GET"


http_handler  = urllib.HTTPHandler(debuglevel=_debug)
https_handler = urllib.HTTPSHandler(debuglevel=_debug)

def twitterreq(url, method, parameters):
  req = oauth.Request.from_consumer_and_token(oauth_consumer,
                                             token=oauth_token,
                                             http_method=http_method,
                                             http_url=url, 
                                             parameters=parameters)

  req.sign_request(signature_method_hmac_sha1, oauth_consumer, oauth_token)

  headers = req.to_header()

  if http_method == "POST":
    encoded_post_data = req.to_postdata()
  else:
    encoded_post_data = None
    url = req.to_url()

  opener = urllib.OpenerDirector()
  opener.add_handler(http_handler)
  opener.add_handler(https_handler)

  response = opener.open(url, encoded_post_data)

  return response

def fetchsamples(max_tweets=0, location=None):
  if location:
    url = "https://stream.twitter.com/1.1/statuses/filter.json?locations=%s" % location
  else:
    url = "https://stream.twitter.com/1.1/statuses/sample.json" 
  parameters = []
  response = twitterreq(url, "GET", parameters)
  count = 0 
  for line in response:
    if 'delete' in json.loads(line):
        continue
    print line.strip()
    count = count + 1
    if max_tweets != 0 and count > max_tweets:
        break

if __name__ == '__main__':
  args = parser.parse_args()
  fetchsamples(max_tweets=args.max_tweets, location=args.location)
