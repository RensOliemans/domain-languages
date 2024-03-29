import logging
from multiprocessing.pool import ThreadPool

from warcio.archiveiterator import ArchiveIterator
import requests
from selectolax.parser import HTMLParser
from langdetect import detect as detect2

from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F
from pyspark.sql.types import StringType


"""
Language detection of Common Crawl webpages with Spark.
This file is big since with spark-submit you can only execute/pass 1 Python file.
Approximate table of contents:

32-38:   configuration
43-156:  mapping : this maps countries+instance to downloaded CDX-index files (should be in hdfs: /user-dir/gzs/CC-MAIN-{instance}--cdx-{country}.gz etc)
159-230: helper functions/classes
233-247: own UDF helper functions (from spark.sql.functions.UDF)
250-282: download_row, which gets a WARC file from the S3 server
284-361: total function, this basically combines everything. It reads the downloaded index files, downloads a warc (by calling download_row) and detects language
         for a given tld+instance (instance refers to a specific CC scrape, such as 2020-50)

"""


logging.basicConfig(level=logging.INFO)
MIN_AMOUNT = 5
MIN_LENGTH = 100
APPNAME = 'Get WARCs'
sc = SparkContext(appName=APPNAME)
sc.setLogLevel('ERROR')
spark = SparkSession.builder.appName(APPNAME).getOrCreate()


# This is hardcoded mapping which keeps track of what index files belong to
# what TLDs (in what years)
MAPPING = {
    '2020-50': {
        'fr': ['00190', '00191', '00192', '00193', '00194'],
        'se': ['00277', '00278', '00279'],
        'de': ['00168', '00169', '00170', '00171', '00172', '00173', '00174', '00175', '00176', '00177', '00178'],
        'it': ['00206', '00207', '00208', '00209', '00210', '00211'],
        'ru': ['00263', '00264', '00265', '00266', '00267', '00268', '00269', '00270', '00271', '00272', '00273',
               '00274', '00275', '00276', '00277'],
        'gr': ['00197', '00198'],
        'uk': ['00289', '00290', '00291', '00292', '00293', '00294', '00295'],
        'es': ['00184', '00185', '00186', '00187'],
    },
    '2020-24': {
        'fr': ['00190', '00191', '00192', '00193', '00194', '00195'],
        'se': ['00279', '00280', '00281'],
        'de': ['00169', '00170', '00171', '00172', '00173', '00174', '00175', '00176', '00177', '00178', '00179',
               '00180'],
        'it': ['00208', '00209', '00210', '00211', '00212', '00213'],
        'ru': ['00266', '00267', '00268', '00269', '00270', '00271', '00272', '00273', '00274', '00275', '00276',
               '00277', '00278', '00279'],
        'gr': ['00197', '00198'],
        'uk': ['00290', '00291', '00292', '00293', '00294', '00295', '00296'],
        'es': ['00184', '00185', '00186', '00187'],
    },
    '2019-47': {
        'fr': ['00197', '00198', '00199', '00200'],
        'se': ['00280', '00281', '00282'],
        'de': ['00177', '00178', '00179', '00180', '00181', '00182', '00183', '00184', '00185', '00186'],
        'it': ['00213', '00214', '00215', '00216', '00217'],
        'ru': ['00269', '00270', '00271', '00272', '00273', '00274', '00275', '00276', '00277', '00278', '00279',
               '00280'],
        'gr': ['00203'],
        'uk': ['00290', '00291', '00292', '00293', '00294', '00295', '00296'],
        'es': ['00191', '00192', '00193', '00194'],
    },
    '2019-22': {
        'fr': ['00187', '00188', '00189', '00190', '00191', '00192', '00193'],
        'se': ['00279', '00280', '00281'],
        'de': ['00165', '00166', '00167', '00168', '00169', '00170', '00171', '00172', '00173', '00174', '00175',
               '00176', '00177'],
        'it': ['00205', '00206', '00207', '00208', '00209', '00210'],
        'ru': ['00264', '00265', '00266', '00267', '00268', '00269', '00270', '00271', '00272', '00273', '00274',
               '00275', '00276', '00277', '00278', '00279'],
        'gr': ['00194', '00195'],
        'uk': ['00289', '00290', '00291', '00292', '00293', '00294', '00295', '00296'],
        'es': ['00181', '00182', '00183', '00184'],
    },
    '2018-47': {
        'fr': ['00182', '00183', '00184', '00185', '00186', '00187'],
        'se': ['00277', '00278', '00279'],
        'de': ['00163', '00164', '00165', '00166', '00167', '00168', '00169', '00170', '00171', '00172', '00173'],
        'it': ['00200', '00201', '00202', '00203', '00204'],
        'ru': ['00258', '00259', '00260', '00261', '00262', '00263', '00264', '00265', '00266', '00267', '00268',
               '00269', '00270', '00271', '00272', '00273', '00274', '00275', '00276', '00277'],
        'gr': ['00188', '00189'],
        'uk': ['00288', '00289', '00290', '00291', '00292', '00293', '00294', '00295'],
        'es': ['00177', '00178', '00179'],
    },
    '2018-22': {
        'fr': ['00194', '00195', '00196', '00197', '00198'],
        'se': ['00279', '00280', '00281'],
        'de': ['00175', '00176', '00177', '00178', '00179', '00180', '00181', '00182', '00183', '00184', '00185',
               '00186'],
        'it': ['00209', '00210', '00211', '00212'],
        'ru': ['00263', '00264', '00265', '00266', '00267', '00268', '00269', '00270', '00271', '00272', '00273',
               '00274', '00275', '00276', '00277', '00278', '00279'],
        'gr': ['00199', '00200'],
        'uk': ['00288', '00289', '00290', '00291', '00292', '00293', '00294', '00295'],
        'es': ['00190', '00191', '00192'],
    },
    '2017-47': {
        'fr': ['00198', '00199', '00200', '00201', '00202'],
        'se': ['00281', '00282', '00283'],
        'de': ['00180', '00181', '00182', '00183', '00184', '00185', '00186', '00187', '00188', '00189'],
        'it': ['00211', '00212', '00213', '00214', '00215'],
        'ru': ['00265', '00266', '00267', '00268', '00269', '00270', '00271', '00272', '00273', '00274', '00275',
               '00276', '00277', '00278', '00279', '00280', '00281'],
        'gr': ['00203', '00204'],
        'uk': ['00290', '00291', '00292', '00293', '00294', '00295', '00296', '00297'],
        'es': ['00193', '00194', '00195', '00196'],
    },
    '2017-22': {
        'fr': ['00203', '00204', '00205', '00206'],
        'se': ['00283', '00284'],
        'de': ['00187', '00188', '00189', '00190', '00191', '00192', '00193', '00194', '00195'],
        'it': ['00215', '00216', '00217', '00218', '00219'],
        'ru': ['00267', '00268', '00269', '00270', '00271', '00272', '00273', '00274', '00275', '00276', '00277',
               '00278', '00279', '00280', '00281', '00282', '00283'],
        'gr': ['00208', '00209'],
        'uk': ['00291', '00292', '00293', '00294', '00295', '00296', '00297'],
        'es': ['00199', '00200', '00201'],
    },
    '2016-44': {
        'fr': ['00199', '00200', '00201', '00202'],
        'se': ['00283', '00284'],
        'de': ['00181', '00182', '00183', '00184', '00185', '00186', '00187', '00188', '00189', '00190'],
        'it': ['00212', '00213', '00214', '00215'],
        'ru': ['00265', '00266', '00267', '00268', '00269', '00270', '00271', '00272', '00273', '00274', '00275',
               '00276', '00277', '00278', '00279', '00280', '00281', '00282', '00283'],
        'gr': ['00205'],
        'uk': ['00291', '00292', '00293', '00294', '00295', '00296', '00297'],
        'es': ['00195', '00196', '00197'],
    },
    '2016-22': {
        'fr': ['00236', '00237'],
        'se': ['00291'],
        'de': ['00221', '00222', '00223', '00224'],
        'it': ['00244', '00245'],
        'ru': ['00291'],
        'gr': ['00242'],
        'uk': ['00292', '00293', '00294', '00295', '00296', '00297', '00298'],
        'es': ['00235', '00236'],
    }
}


def get_text_from_html(html):
    """
    Uses Selectolax to parse the HTML
    """
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
    return len(longest_lines) < MIN_AMOUNT or any([len(line) < MIN_LENGTH for line in longest_lines])


class Item(object):
    def __init__(self, obj):
        # self.url = obj.data['url']
        self.content = obj


class ParsedItem(object):
    """Item with easy parse and longest-sentence functionality"""
    def __init__(self, item, parser=get_text_from_html):
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
        super(FilteredItem, self).__init__(item, parser)
        self.filters = [filter_text] if filters is None else filters

    @property
    def filter_out(self):
        return self.parsed is None or any((f(self.parsed) for f in self.filters))


def convert_to_tld(entry):
    """Converts the URL-info (from CC) into a TLD"""
    parts = entry.split(',')
    return parts[0]


def strip_dq(entry):
    """Strips double quotes from CC data"""
    if entry:
        return entry.replace('"', '')
    return entry


udf_tld = F.udf(convert_to_tld, StringType())
udf_strip_dq = F.udf(strip_dq, StringType())


def download_row(row, language, prefix):
    """
    This method gets the filtered WARC result.

    row:      row object, what is returned bhe CC index. Contains filename, length, offset
    language: language currently used, not really necessary but useful for data structure
    prefix:   prefix of the S3 dataset, usually https://commoncrawl.s3.amazonaws.com/
    """
    url = prefix + row.filename
    url = url.replace(',', '').replace('}', '')  # Strip infrequent last characters

    # This is necessary since the CC data sometimes (but not always) contains a comma
    offset = int(row.offset.replace(',', ''))
    end = offset + int(row.length.replace(',', ''))  # end = offset + length

    headers = {"Range": "bytes={}-{}".format(offset, end)}

    try:
        # Download WARC bestand voor dit specifieke domein
        resp = requests.get(url, headers=headers, stream=True)

        for record in ArchiveIterator(resp.raw, arc2warc=True):
            try:
                item = FilteredItem(Item(record.content_stream().read()))
                if not item.filter_out:
                    if item.to_detect == '':
                        raise Exception('Empty item %s %s %s' % (item.filter_out, item.to_detect, url))
                    return language, item.to_detect
            except Exception as e:
                logging.debug('Skipping record %s, got exception %s', record, e)
    except Exception as e:
        logging.info('Warc Error: %s', e)
        return


def detect(text):
    try:
        return pipeline.annotate(content)['language'][0]
    except Exception:
        return 'empty'


def detect_lang(text):
    try:
        return detect2(text)
    except Exception:
        return 'empty'


def total(language, instance):
    logging.info('Starting with extracting gz files. language: %s, instance %s', language, instance)
    relevant_files = MAPPING[instance][language]

    logging.info(relevant_files)
    # These are the downloaded index files, each a bit less than 1G in size and have to be downloaded using get_index_files
    files_to_read = ['gzs/CC-MAIN-{}--cdx-{}.gz'.format(instance, i) for i in relevant_files]
    logging.info(files_to_read)

    # Load CSV
    df = spark.read.csv(files_to_read, sep=' ').repartition(100)

    logging.info('Read files')

    # Take relevant columns and rename
    # df.show()
    df = df.select('_c0', '_c13', '_c15', '_c17') \
        .withColumnRenamed('_c0', 'urlinfo') \
        .withColumnRenamed('_c13', 'length') \
        .withColumnRenamed('_c15', 'offset') \
        .withColumnRenamed('_c17', 'filename')

    # df.show()
    # Filter null values
    df = df.na.drop()

    # Filter language
    df = df.filter((df.urlinfo.startswith('{},'.format(language))))

    # Strip double quotes
    df = df \
        .withColumn('length', udf_strip_dq(df.length)) \
        .withColumn('offset', udf_strip_dq(df.offset)) \
        .withColumn('filename', udf_strip_dq(df.filename))

    # Keep only tld
    df = df \
        .withColumn('urlinfo', udf_tld(df.urlinfo)) \
        .withColumnRenamed('urlinfo', 'tld')

    # Sample
    fraction = 0.01
    logging.info('Using fraction: %s%%', fraction * 100)
    df = df.sample(fraction)

    prefix = 'https://commoncrawl.s3.amazonaws.com/'

    total_languages = ['bg', 'cs', 'de', 'el', 'en', 'es', 'fi', 'fr', 'hr', 'hu',
                       'it', 'no', 'pl', 'pt', 'ro', 'ru', 'sk', 'sv', 'tr', 'uk', 'empty']

    rdd2 = df.rdd.map(lambda row: '{}{} -- {} -- {}'.format(prefix, row.filename, row.offset, row.length))
    # Convert in form, detect language and combine results
    rdd = df.rdd \
        .map(lambda row: download_row(row, language, prefix)).filter(bool) \
        .map(lambda cl: (cl[0], detect_lang(cl[1]))) \
        .map(lambda cl: (cl[0], {lang: 1 if lang == cl[1] else 0 for lang in total_languages})) \
        .reduceByKey(lambda a, b: {c: a[c] + b[c] for c in total_languages})

    # Collect() here is fine: these are the final results
    logging.info('Getting results for country %s', language)
    results = rdd.collect()
    logging.critical('For country %s, results %s (%s rows)', language, results, len(results))
    print('For country: %s, instance %s, results: %s' % (language, instance, results))


# languages = ['se', 'it', 'es', 'ru', 'gr', 'de', 'uk', 'fr']

# Execute on a specific language and instance
total('fr', '2017-47')
