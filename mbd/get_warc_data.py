from warcio.archiveiterator import ArchiveIterator
import requests
from selectolax.parser import HTMLParser

from pyspark import SparkContext
from pyspark.sql import SparkSession

import pyspark.sql.functions as F
from pyspark.sql.types import *


MIN_AMOUNT = 5
MIN_LENGTH = 100
APPNAME = 'Get WARCs'
sc = SparkContext(appName=APPNAME)
sc.setLogLevel('ERROR')
spark = SparkSession.builder.appName(APPNAME).getOrCreate()


def get_text_from_html(html):
    tree = HTMLParser(html)

    if tree.body is None:
        return None

    for tag in tree.css(u'script'):
        tag.decompose()
    for tag in tree.css(u'style'):
        tag.decompose()

    text = tree.body.text(separator=u'\n')
    return text


def get_longest_sentence(text):
    text = text.replace('\t', '')
    return sorted(map(lambda t: t.strip(), text.split('\n')), key=len)[-MIN_AMOUNT:]


def filter_text(text):
    longest_lines = get_longest_sentence(text)
    text = text.split('\n')
    return len(text) < MIN_AMOUNT or any([len(line) < MIN_LENGTH for line in longest_lines])


class Item:
    def __init__(self, obj):
        self.url = obj.data['url']
        self.content = obj.content


class ParsedItem:
    def __init__(self, item: Item, parser=get_text_from_html):
        self.item = item
        self._parser = parser
        self._parsed = None
        self._longest_sentence = None

    @property
    def parsed(self):
        if self._parsed is None:
            self._parsed = self._parser(self.item.content)
        return self._parsed

    @property
    def url(self):
        return self.item.url

    @property
    def longest_sentences(self):
        if self._longest_sentence is None:
            self._longest_sentence = get_longest_sentence(self.parsed)
        return self._longest_sentence

    @property
    def to_detect(self):
        return ' '.join(self.longest_sentences)


class FilteredItem(ParsedItem):
    def __init__(self, item, parser=get_text_from_html, filters=None):
        super().__init__(item, parser)
        self.filters = [filter_text] if filters is None else filters

    @property
    def filter_out(self):
        return self.parsed is None or any((f(self.parsed) for f in self.filters))


LANGUAGE = 'fr'
INSTANCE = '2020-50'
directory = 'output2/CC-MAIN-{}-{}'.format(INSTANCE, LANGUAGE)

PREFIX = 'https://commoncrawl.s3.amazonaws.com/'

df = spark.read.option('header', 'true').csv(directory)
df = df.collect()

for i, row in enumerate(df):
    url = PREFIX + row.filename
    out_filename = '{}.warc.gz'.format(i)
    url = url.replace(',', '').replace('}', '')

    offset = int(row.offset.replace(',', ''))
    end = offset + int(row.length.replace(',', ''))

    headers = {"Range": "bytes={}-{}".format(offset, end)}

    print('Downloading file %s, range %s' % (url, headers))
    resp = requests.get(url, headers=headers, stream=True)

    for record in ArchiveIterator(resp.raw, arc2warc=True):
        if record.rec_type == 'response':
            if record.http_headers.get_header('Content-Type') == 'text/html':
                item = FilteredItem(Item(record.content_stream().read()))
                if not item.filter_out:
                    print(item.to_detect)
                print('')

