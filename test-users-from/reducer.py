#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys


def main():
    current_word = ""
    count = 0
    for word in sys.stdin:
        if word == current_word:
            count = count + 1
            continue
        print "%s - %d" % (current_word, count)
        current_word = word
        count = 1

if __name__ == "__main__":
    main()
