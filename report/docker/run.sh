#/bin/bash

# while true; do python twitterstream.py $MAX_TWEETS_PER_BATCH > /output/$(date '+%Y%m%d_%H%M'); sleep $(($SLEEP_TIME * 60)); done
while true; do python twitterstream.py --max_tweets $MAX_TWEETS_PER_BATCH --location=$LOCATION_FILTER > /output/$(date '+%Y%m%d_%H%M'); sleep $(($SLEEP_TIME * 60)); done 
