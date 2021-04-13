import os
import logging
from multiprocessing.pool import ThreadPool

from warcio.archiveiterator import ArchiveIterator
import requests
from selectolax.parser import HTMLParser

# from pyspark import SparkContext
# from pyspark.sql import SparkSession, Row
# import pyspark.sql.functions as F
# from pyspark.sql.types import StringType


logging.basicConfig(level=logging.INFO)
MIN_AMOUNT = 5
MIN_LENGTH = 100
APPNAME = 'Get WARCs'
# sc = SparkContext(appName=APPNAME)
# sc.setLogLevel('ERROR')
# spark = SparkSession.builder.appName(APPNAME).getOrCreate()


def main(languages, instances):
    for language in languages:
        # First, download the .gz files. These will be stored on disk which can later be used to download the WARC files
        download_files(language, instances)
    # output(languages,  instances)


# ------------------------  DOWNLOADING .GZ FILES -----------------------
def download_files(pattern, instances):
    prefix = 'https://commoncrawl.s3.amazonaws.com/'

    print('Getting file urls, possibly downloading cluster files for it')
    file_urls_per_instance = get_file_urls(prefix, instances, '{},'.format(pattern))

    print('Getting gz_files for instance')
    for file_urls in file_urls_per_instance:
        # Each file url is a url where to download a .gz file
        gz_filenames = get_gz_files(file_urls)
        print(list(gz_filenames))


def get_file_urls(prefix, instances, pattern):
    """
    This saves cluster.idx files (gets them from index.commoncrawl.org) and then
    finds what parts of the index belong to a given pattern. Example:

    'fr' is covered in the cluster in files: 00190.gz, 00191.gz, 00192.gz, 00193.gz and 00194.gz
    This method then returns something like
    [https://commoncrawl.s3.amazonaws.com/cc-index/collection/CC-MAIN-2020-50/indexes/00190.gz,
     https://commoncrawl.s3.amazonaws.com/cc-index/collection/CC-MAIN-2020-50/indexes/00191.gz,
     etc]
    which are ready to download and store on disk

    """
    save_cluster_files(prefix, instances)  # cluster.idx

    for instance in instances:
        print('Getting urls from cluster %s with pattern %s' % (instance, pattern))
        dg = DomainGetter(prefix, instance, 'clusters/cluster-{}.idx'.format(instance))
        yield dg.get_urls(pattern)


def save_cluster_files(prefix, instances):
    """
    This method takes a lot of cluster.idx files (~200MB each, these are a 'summary' of an index server).
    For each cluster.idx file, it downloads them and stores them if they don't exist yet,
    and does nothing if they do exit
    """
    for instance in instances:
        full_instance = 'CC-MAIN-' + instance
        cfg = ClusterFileSaver(prefix, full_instance, 'clusters/', 'cluster-{}.idx'.format(instance))
        if not cfg.exists:
            print('Saving file %s' % full_instance)
            cfg.save()
        else:
            print('File %s already exists' % full_instance)


def get_gz_files(file_urls):
    """
    This downloads a .gz file from a given url and stores it. For example, if you give
     'https://commoncrawl.s3.amazonaws.com/.../CC-MAIN-2020-50/.../00190.gz,
    this file stores the file on disk and returns the location of where it was stored.
    """
    for file_url in file_urls:
        parts = file_url.split('/')
        filename = parts[-1]
        instance = parts[-3]
        out_filename = 'gzs/{}--{}'.format(instance, filename)

        if os.path.isfile(out_filename):
            print('File %s already exists' % out_filename)
            yield out_filename
            continue

        with requests.get(file_url, stream=True) as r:
            print('Downloading file %s' % file_url)
            r.raise_for_status()
            with open(out_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        yield out_filename


class DomainGetter:
    def __init__(self, prefix, instance, cluster_filename):
        self.prefix = prefix
        self.instance = instance
        self.cluster_filename = cluster_filename

    def get_urls(self, pattern):
        urls = ('{}cc-index/collections/CC-MAIN-{}/indexes/{}'
                .format(self.prefix, self.instance, f)
                for f in self._get_filenames(pattern))
        for url in urls:
            yield url

    def _get_filenames(self, pattern):
        with open(self.cluster_filename, 'r') as f:
            return sorted(list({self._get_filename(line) for line in f if line.startswith(pattern)}))

    @staticmethod
    def _get_filename(line):
        return line.split('\t')[1]


class ClusterFileSaver:
    def __init__(self, prefix, instance, location, filename):
        self.url = self.get_url(prefix, instance)
        self.file_location = location + filename

    def save(self):
        with requests.get(self.url, stream=True) as r:
            r.raise_for_status()
            with open(self.file_location, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return self.file_location

    @property
    def exists(self):
        return os.path.isfile(self.file_location)

    @staticmethod
    def get_url(prefix, instance):
        return '{}cc-index/collections/{}/indexes/cluster.idx'.format(prefix, instance)


# --------------------- GOING FROM .GZ FILES TO WARC FILES -------------------
def output(languages, instances):
    """Basically: download WARC files for all languages and instances"""
    for instance in instances:
        p = ThreadPool(8)
        p.map(lambda l: combined(l, instance), languages)
        p.close()
        p.join()


def combined(language, instance):
    extract_gzs(language, instance)
    store_warcs(language, instance)


def extract_gzs(language, instance):
    """
    This function takes a lot of .gz files and filters out unnecessary data that is in them.
    We only need the TLD, filename (where to download the WARC), and the length and offset.
    Discard the rest
    """
    logging.info('Starting with extracting gz files. language: %s, instance %s', language, instance)
    relevant_files = MAPPING[instance][language]

    relevant_files = ['gzs/CC-MAIN-{}'.format(i) for i in relevant_files]
    logging.info(relevant_files)

    # Load CSV
    df = spark.read.csv('gzs', sep=' ').repartition(100)

    logging.info('Read files')

    # Take relevant columns and rename
    df = df.select('_c0', '_c13', '_c15', '_c17') \
        .withColumnRenamed('_c0', 'urlinfo') \
        .withColumnRenamed('_c13', 'length') \
        .withColumnRenamed('_c15', 'offset') \
        .withColumnRenamed('_c17', 'filename')

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

    # df = df.sample(0.01)
    logging.info('Saving gz files')
    df.write.format('parquet').mode('overwrite').option('header', 'true').csv('warc-locations/{}-{}'.format(instance, language))
    logging.info('Saved gz files')


def store_warcs(language, instance):
    """
    This method reads files from disk (which we previously put in the correct format)
    and then downloads the WARC files for them. It then writes that back to disk again.
    """
    logging.info('Starting with storing WARCs. language: %s, instance %s', language, instance)
    directory = 'warc-locations/CC-MAIN-{}-{}'.format(instance, language).replace(';', '')
    out_filename = 'output/{}-{}'.format(instance, language)

    prefix = 'https://commoncrawl.s3.amazonaws.com/'

    fraction = 0.01
    logging.info('Reading %s%% of %s', fraction * 100, directory)
    schema = ['tld', 'content']

    df = spark.read.option('header', 'true').csv(directory).sample(fraction)
    rdd = df.rdd.map(lambda row: download_row(row, language, prefix)).filter(bool)
    df = spark.createDataFrame(rdd, schema)
    df.write.format('parquet').mode('overwrite').option('header', 'true').csv(out_filename)
    logging.info('Stored in %s', out_filename)
    logging.info('Amount of pages: %s', df.count())


def download_row(row, language, prefix):
    """
    This method gets the filtered WARC result.

    row:      row object, what is returned bhe CC index. Contains filename, length, offset
    language: language currently used, not really necessary but useful for data structure
    prefix:   prefix of the S3 dataset, usually https://commoncrawl.s3.amazonaws.com/
    """
    url = prefix + row.filename
    url = url.replace(',', '').replace('}', '')

    offset = int(row.offset.replace(',', ''))
    end = offset + int(row.length.replace(',', ''))

    headers = {"Range": "bytes={}-{}".format(offset, end)}

    logging.info('Downloading file %s, range %s', url, headers)
    try:
        resp = requests.get(url, headers=headers, stream=True)

        for record in ArchiveIterator(resp.raw, arc2warc=True):
            if record.rec_type == 'response':
                if record.http_headers.get_header('Content-Type') == 'text/html':
                    item = FilteredItem(Item(record.content_stream().read()))
                    if not item.filter_out:
                        return language, item.to_detect
                    logging.info('\n')
    except ConnectionError as e:
        logging.info('Connection Error: %s', e)
        return


# ------------------    HELPER FUNCTIONS -----------------------
def convert_to_tld(entry):
    parts = entry.split(',')
    return parts[0]


def strip_dq(entry):
    if entry:
        return entry.replace('"', '')
    return entry


# udf_tld = F.udf(convert_to_tld, StringType())
# udf_strip_dq = F.udf(strip_dq, StringType())


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


class Item(object):
    def __init__(self, obj):
        # self.url = obj.data['url']
        self.content = obj


class ParsedItem(object):
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


main(['se', 'it', 'es', 'ru', 'gr', 'de', 'uk', 'fr'], ['2017-47'])


"""
Stappen:
    1. Download gz bestanden (index)
    2. Transformeer gz bestanden naar interne CSV bestanden, gooi irrelevante data weg
    3. Uit de nieuwe CSV bestanden, downlaod WARC gegevens
"""

