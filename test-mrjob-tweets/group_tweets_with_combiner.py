from datetime import datetime
from mrjob.job import MRJob
import json


FILTER_COMMAND = ''' grep '"country_code":"ES"' '''


class MyJob(MRJob):
    def mapper_pre_filter(self):
        return FILTER_COMMAND

    def mapper(self, _, line):
        json_tweet = json.loads(line)
        ts = datetime.fromtimestamp(int(json_tweet['timestamp_ms'])/1000)
        yield ("YEAR", ts.year), 1
        yield ("MONTH", ts.month), 1
        yield ("DAY", ts.day), 1

    def combiner(self, key, value):
        yield key, (sum(value), sum(value))
        yield key, (sum(value), sum(value))
        yield key, (sum(value), sum(value))

    def reducer(self, key, value):
        yield key, sum([v[0] for v in value])

if __name__ == '__main__':
    MyJob.run()
