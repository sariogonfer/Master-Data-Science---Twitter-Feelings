from mrjob.job import MRJob
import json


class CountWords(MRJob):
    def mapper(self, _, line):
        foo = json.loads(line)
        if 'text' in foo:
            for word in foo['text'].split():
                yield word, 1

    def combiner(self, key, it):
        yield key, sum(it)

    def reducer(self, key, it):
        yield key, sum(it)

if __name__ == '__main__':
    print "Start"
    CountWords.run()
