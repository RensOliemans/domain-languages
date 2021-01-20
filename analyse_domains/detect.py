from pyspark import SparkContext
from pyspark.sql import SparkSession

import sparknlp
from sparknlp.pretrained import PretrainedPipeline

from langdetect import detect

APPNAME = 'detect-language'
PIPELINE = ('detect_language_20', 'xx')

sc = SparkContext(appName=APPNAME)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.appName(APPNAME).getOrCreate()
spark2 = sparknlp.start()

pipeline = PretrainedPipeline(PIPELINE[0], lang=PIPELINE[1])

COUNTRIES = ['de', 'fr', 'es', 'uk', 'se', 'it', 'ru', 'gr']
LANGUAGES = ['bg', 'cs', 'de', 'el', 'en', 'es', 'fi', 'fr', 'hr', 'hu',
             'it', 'no', 'pl', 'pt', 'ro', 'ru', 'sk', 'sv', 'tr', 'uk']
COUNTRY = 'fr'
df = spark.read.option('header', 'true').csv('small_' + COUNTRY)

rdd = df.rdd
rdd = rdd.map(lambda ccu: (ccu[1], detect(ccu[0])))
print(rdd.take(10))
rdd = rdd.map(lambda cl: (cl[0], {lang: 1 if lang == cl[1] else 0 for lang in LANGUAGES}))
print(rdd.take(10))

rdd = rdd.reduceByKey(lambda a, b: {c: a[c] + b[c] for c in LANGUAGES})

print(rdd.collect())

# tuple unpacking
# rdd = df.rdd \
#     .map(lambda (content, country, url): (country, pipeline.annotate(content)['language'][0])) \
#     .map(lambda (country, language): (country, {lang: 1 if lang == language else 0 for lang in LANGUAGES})) \
#     .reduceByKey(lambda a, b: {c: a[c] + b[c] for c in LANGUAGES})
