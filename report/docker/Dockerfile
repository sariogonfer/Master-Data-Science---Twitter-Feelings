FROM resin/raspberry-pi-alpine-python

COPY ./twitterstream.py /twitterstream.py
COPY ./run.sh /run.sh

RUN chmod a+x /run.sh
RUN pip install oauth2

ENV ACCESS_TOKEN_KEY=""
ENV ACCESS_TOKEN_SECRET=""
ENV CONSUMER_KEY=""
ENV CONSUMER_SECRET=""
ENV SLEEP_TIME=60
ENV MAX_TWEETS_PER_BATCH=10000
ENV LOCATION_FILTER=""

VOLUME /output

CMD /run.sh
