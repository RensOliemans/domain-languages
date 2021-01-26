from pyspark import SparkContext
from pyspark.sql import SparkSession

import pyspark.sql.functions as F
from pyspark.sql.types import *

LANGUAGE = 'se'


APPNAME = 'Filter URLs'
sc = SparkContext(appName=APPNAME)
sc.setLogLevel('ERROR')
spark = SparkSession.builder.appName(APPNAME).getOrCreate()


def convert_to_tld( entry):
    parts = entry.split(',')
    return parts[0]


def strip_dq(entry):
    if entry:
        return entry.replace('"', '')
    return entry


udf_tld = F.udf(convert_to_tld, StringType())
udf_strip_dq = F.udf(strip_dq, StringType())


MAPPING = {
    '2020-50': {
        'fr': ['00190', '00191', '00192', '00193', '00194'],
        'se': ['00277', '00278', '00279']
    },
    '2020-24': {
        'fr': ['00190', '00191', '00192', '00193', '00194', '00195'],
        'se': ['00279', '00280', '00281'],
    },
    '2019-47': {
        'fr': ['00197', '00198', '00199', '00200'],
        'se': ['00280', '00281', '00282'],
    },
    '2019-22': {
        'fr': ['00187', '00188', '00189', '00190', '00191', '00192', '00193'],
        'se': ['00279', '00280', '00281'],
    },
    '2018-47': {
        'fr': ['00182', '00183', '00184', '00185', '00186', '00187'],
        'se': ['00277', '00278', '00279'],
    },
    '2018-22': {
        'fr': ['00194', '00195', '00196', '00197', '00198'],
        'se': ['00279', '00280', '00281'],
    },
    '2017-47': {
        'fr': ['00198', '00199', '00200', '00201', '00202'],
        'se': ['00281', '00282', '00283'],
    },
    '2017-22': {
        'fr': ['00203', '00204', '00205', '00206'],
        'se': ['00283', '00284'],
    },
    '2016-44': {
        'fr': ['00199', '00200', '00201', '00202'],
        'se': ['00283', '00284'],
    },
    '2016-22': {
        'fr': ['00236', '00237'],
        'se': ['00291'],
    }
}


instances = ['2020-50']
instances = ['gzs/CC-MAIN-{}'.format(i) for i in instances]

for instance in instances:
    print('Using instance %s' % instance)

    # Load CSV
    df = spark.read.csv('{}--*.gz'.format(instance), sep=' ')

    df = df.repartition(25)

    # Take relevant columns and rename
    df = df.select('_c0', '_c3', '_c5', '_c13', '_c15', '_c17') \
        .withColumnRenamed('_c0', 'urlinfo') \
        .withColumnRenamed('_c3', 'url') \
        .withColumnRenamed('_c5', 'mime') \
        .withColumnRenamed('_c13', 'length') \
        .withColumnRenamed('_c15', 'offset') \
        .withColumnRenamed('_c17', 'filename')

    # Filter null values
    df = df.na.drop()

    # Filter language
    df = df.filter((df.urlinfo.startswith('{},'.format(LANGUAGE))))

    # Strip double quotes
    df = df \
        .withColumn('url', udf_strip_dq(df.url)) \
        .withColumn('length', udf_strip_dq(df.length)) \
        .withColumn('offset', udf_strip_dq(df.offset)) \
        .withColumn('filename', udf_strip_dq(df.filename))

    # Keep only tld
    df = df \
        .withColumn('urlinfo', udf_tld(df.urlinfo)) \
        .withColumnRenamed('urlinfo', 'tld')

    # Cast to int
    df = df \
        .withColumn('length', df.length.cast('int')) \
        .withColumn('offset', df.offset.cast('int'))

    df.write.format('parquet').mode('overwrite').option('header', 'true').csv('output/{}-{}'.format(instance.split('/')[1], LANGUAGE))

