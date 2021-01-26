from pyspark import SparkContext
from pyspark.sql import SparkSession

import pyspark.sql.functions as F
from pyspark.sql.types import *


APPNAME = 'Filter URLs'
sc = SparkContext(appName=APPNAME)
sc.setLogLevel('ERROR')
spark = SparkSession.builder.appName(APPNAME).getOrCreate()


instances = ['2020-50']
instances = ['gz/CC-MAIN-{}'.format(i) for i in instances]


def split_on_colon(entry):
    parts = entry.split(':')
    if len(parts) > 0:
        return parts[0]
    return parts

splitUdf = F.udf(split_on_colon, StringType())

def convertToTld(entry):
    parts = entry.split(',')
    return parts[0]

tldUdf = F.udf(convertToTld, StringType())


def stripDQ(entry):
    return entry.replace('"', '')
udf_stripDQ = F.udf(stripDQ, StringType())


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

# Filter language
df = df.filter((df.urlinfo.startswith('fr,')))

# Strip double quotes
df = df \
    .withColumn('url', udf_stripDQ(df.url)) \
    .withColumn('mime', udf_stripDQ(df.mime)) \
    .withColumn('length', udf_stripDQ(df.length)) \
    .withColumn('offset', udf_stripDQ(df.offset)) \
    .withColumn('filename', udf_stripDQ(df.filename))

# Cast to int
df = df \
    .withColumn('length', df.length.cast('int')) \
    .withColumn('offset', df.offset.cast('int'))

