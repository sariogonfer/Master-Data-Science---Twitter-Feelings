from datetime import datetime
import json
import pickle
import re
import string

from mrjob.job import MRJob
from mrjob.step import MRStep


''' Filter command'''
FILTER_CMD = '''grep '"country_code":"ES"' '''

ALLOWED_COUNTRY_CODES = ['ES']

''' Labels definition '''
COUNTRY_LABEL = 'COUNTRY'
DATE_LABEL = 'DATE'
ERROR_LABEL = 'ERROR'
HASHTAG_LABEL = 'HASHTAG'
HOUR_LABEL = 'HOUR'
REGION_LABEL = 'REGION'

''' Error labels '''
OTHER_ERROR = 'OTHER'
UNKNOW_REGION_ERROR = 'UNKNOW_REGION'
NOT_SCORED_TEXT_ERROR = 'NOT_SCORED'

''' Top happiest '''
MIN_COUNT_TWEET_FOR_TOP = 2
TOP_COUNT = 10
TOP_LABELS = [HASHTAG_LABEL, ]


class ZeroValidWordsException(Exception):
    pass


class RegionNotFoundException(Exception):
    pass

class NotValidLine(Exception):
    pass


class RegionByTown(object):
    region_by_town = None

    def __init__(self, path):
        with open(path, 'rb') as f:
            self.region_by_town = pickle.load(f)
        super(RegionByTown, self).__init__()

    def __call__(self, town):
        cleaned_town = re.sub('[\']', '', town).replace(' ', '').lower()
        for aux in cleaned_town.split('-'):
            if aux in self.region_by_town:
                return self.region_by_town[cleaned_town]
        raise RegionNotFoundException

class TextFeelingCalculator(object):
    score_by_word = None

    def __init__(self, path):
        with open(path, 'rb') as f:
            self.score_by_word = pickle.load(f)
        super(TextFeelingCalculator, self).__init__()

    def __call__(self, raw_text):
        splited_text = re.sub('[%s]' % re.escape(string.punctuation), ' ',
                              raw_text).split(' ')
        scores = [self.score_by_word[word] for word in splited_text
                  if word in self.score_by_word]
        try:
            return sum(scores) / len(scores)
        except ZeroDivisionError:
            raise ZeroValidWordsException


class MyOutputProtocol(object):
    def write(self, key, data):
        try:
            label = key[0]
            value = key[1].encode('ascii', errors='replace')
            count = str(data[0])
            score = str(data[1])

            return ','.join([label, value, count, score])
        except Exception as exc:
            return ''


class MyJob(MRJob):
    OUTPUT_PROTOCOL = MyOutputProtocol
    score_calculator = None
    region_by_town = None

    def __init__(self, *args, **kwargs):
        self.word_scores_pickle_path = 'pickles/pickles/words.pickle'
        self.rel_town_region_pickle_path = 'pickles/pickles/rel_town_region.pickle'
        super(MyJob, self).__init__(*args, **kwargs)

    def mapper_init(self):
        self.score_calculator = TextFeelingCalculator(
            self.word_scores_pickle_path)
        self.region_by_town = RegionByTown(self.rel_town_region_pickle_path)

    def mapper(self, _, raw_tweet):
        try:
            json_tweet = json.loads(raw_tweet)
            try:
                if json_tweet['place']['country_code'] not in \
                        ALLOWED_COUNTRY_CODES:
                    raise NotValidLine
            except KeyError:
                raise NotValidLine
            try:
                text = json_tweet['extended_text']['full_text']
            except KeyError:
                text = json_tweet['text']
            score = self.score_calculator(text)
            region = self.region_by_town(
                json_tweet['place']['name'].encode('utf8'))
            ts = datetime.fromtimestamp(int(json_tweet['timestamp_ms'])/1000)
            date_str = ts.strftime('%Y%m%d')
            hour_str = str(ts.hour)
            for hashtag in json_tweet['entities']['hashtags']:
                yield (HASHTAG_LABEL, hashtag['text']), score
            yield (REGION_LABEL, region), score
            yield (HOUR_LABEL, hour_str), score
            yield (DATE_LABEL, date_str), score
        except ZeroValidWordsException as exc:
            yield (ERROR_LABEL, NOT_SCORED_TEXT_ERROR), 1
        except RegionNotFoundException:
            yield (ERROR_LABEL, UNKNOW_REGION_ERROR), 1
        except NotValidLine:
            pass
        except Exception as exc:
            yield (ERROR_LABEL, OTHER_ERROR), 1

    def combiner(self, key, scores):
        count = 0
        sum_ = 0
        for score in scores:
            count += 1
            sum_ += score
        yield key, (count, sum_)

    def reducer(self, key, scores):
        count = 0
        sum_ = 0
        for score in scores:
            count += score[0]
            sum_ += score[1]
        yield key, (count, sum_ / count)

    def top_mapper(self, key, score):
        if key[0] in TOP_LABELS:
            if score[0] >= MIN_COUNT_TWEET_FOR_TOP:
                yield key[0], (score[1], score[0], key[1], )
        else:
            yield key, score

    def top_reducer(self, key, iter_):
        if key in TOP_LABELS:
            for elem in sorted(list(iter_), reverse=True)[:TOP_COUNT]:
                yield (key, elem[2]), (elem[1], elem[0])
        else:
            yield key, iter_.next()

    def steps(self):
        return [MRStep(mapper_init=self.mapper_init,
                       mapper=self.mapper,
                       combiner=self.combiner,
                       reducer=self.reducer),
                MRStep(mapper=self.top_mapper,
                       reducer=self.top_reducer), ]

if __name__ == '__main__':
    MyJob().run()
