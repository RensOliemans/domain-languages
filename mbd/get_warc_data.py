import requests

from pyspark import SparkContext
from pyspark.sql import SparkSession

import pyspark.sql.functions as F
from pyspark.sql.types import *


APPNAME = 'Get WARCs'
sc = SparkContext(appName=APPNAME)
sc.setLogLevel('ERROR')
spark = SparkSession.builder.appName(APPNAME).getOrCreate()


LANGUAGE = 'fr'
INSTANCE = '2020-50'
directory = 'output2/CC-MAIN-{}-{}'.format(INSTANCE, LANGUAGE)

PREFIX = 'https://commoncrawl.s3.amazonaws.com/'

df = spark.read.option('header', 'true').csv(directory)
# print(df.take(10))
df = df.collect()

for i, row in enumerate(df):
    url = PREFIX + row.filename
    out_filename = '{}.warc.gz'.format(i)

    with requests.get(url, stream=True) as r:
        print('Downloading file %s' % url)
        r.raise_for_status()
        with open(out_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print('Written file %s' % out_filename)
