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
    return entry.replace('"', '')


udf_tld = F.udf(convert_to_tld, StringType())
udf_strip_dq = F.udf(strip_dq, StringType())


MAPPING = {
    '2020-50': {
        'fr': ['00190', '00191', '00192', '00193', '00194'],
        'se': ['00277', '00278', '00279']
    }
}


instances = ['2020-50']
instances = ['gzs/CC-MAIN-{}'.format(i) for i in instances]

for instance in instances:
    print('Using instance %s' % instance)

    # Load CSV
    df = spark.read.csv('{}--*.gz'.format(instance), sep=' ')

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

    df.write.csv('output/{}-{}'.format(instance.split('/')[1], LANGUAGE))

