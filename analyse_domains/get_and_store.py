import logging
from multiprocessing.pool import ThreadPool

import cdx_toolkit
from selectolax.parser import HTMLParser
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row

logging.basicConfig(level=logging.INFO)

# COUNTRY = 'fr'
# URL = f'*.{COUNTRY}'
LIMIT = int(10e3)
MIN_AMOUNT = 5
MIN_LENGTH = 100
APPNAME = 'get CC data'

sc = SparkContext(appName=APPNAME)
sc.setLogLevel("ERROR")
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


class Fetcher:
    def __init__(self, url, limit=100, source='cc', warc_url_prefix=None):
        self.url = url
        self.limit = limit
        self._cdx = cdx_toolkit.CDXFetcher(source=source, warc_url_prefix=warc_url_prefix)

    @property
    def objects(self):
        for obj in self._cdx.iter(self.url, limit=self.limit, filter=["!~robots.txt", "mime:text/html"]):
            yield Item(obj)


def _get_filtered_items(objects):
    for i, item in enumerate(objects):
        if i % 100 == 0:
            logging.info('url %s: %s', i, item.url)
        yield FilteredItem(item)


def fetch(country):
    url = f'*.{country}'

    logging.info('started fetching for country %s', country)

    fetcher = Fetcher(url, LIMIT, source='https://index.commoncrawl.org/CC-MAIN-2015-14-index',
                      warc_url_prefix='https://commoncrawl.s3.amazonaws.com')

    data = [{'url': item.url, 'content': item.to_detect, 'country': country}
            for item in _get_filtered_items(fetcher.objects)
            if not item.filter_out]

    schema = ['url', 'content', 'country']
    try:
        df = spark.createDataFrame(sc.parallelize((Row(**x) for x in data), numSlices=LIMIT / 10), schema)
        df.write.format('parquet').mode('overwrite').option('header', 'true').csv(country)

        logging.info('Storing %s items for url %s. File: %s', len(data), url, country)
    except Exception as e:
        logging.info('Couldnt store for country %s, got error %s', country, e)
        return


def main():
    COUNTRIES = ['de', 'fr', 'es', 'uk', 'se', 'it', 'ru', 'gr']
    p = ThreadPool(8)
    p.map(fetch, COUNTRIES)
    p.close()
    p.join()


if __name__ == '__main__':
    main()
