#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import sys


def read_input(in_):
    for line in in_:
        yield line


def filter_valid_tweets(iter_in):
    for str_in in iter_in:
        tweet = json.loads(str_in)
        if 'delete' not in tweet:
            yield tweet

def read_filtered_tweets(in_):
    for tweet in filter_valid_tweets(read_input(in_)):
        yield tweet

def main():
    for tweet in read_filtered_tweets(sys.stdin):
        try:
            print tweet['user']['location']
        except:
            print "Codec error"

if __name__ == "__main__":
    main()
