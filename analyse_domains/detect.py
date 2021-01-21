from multiprocessing.pool import ThreadPool

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

# pipeline = PretrainedPipeline(PIPELINE[0], lang=PIPELINE[1])

COUNTRIES = ['de', 'fr', 'es', 'uk', 'se', 'it', 'ru', 'gr']
LANGUAGES = ['bg', 'cs', 'de', 'el', 'en', 'es', 'fi', 'fr', 'hr', 'hu',
             'it', 'no', 'pl', 'pt', 'ro', 'ru', 'sk', 'sv', 'tr', 'uk']


def main(country):
    print('country')
    try:
        df = spark.read.option('header', 'true').csv(country)
    except Exception:
        print(f'No files found under country {country}')
        return

    rdd = df.rdd
    rdd = rdd.map(lambda ccu: (ccu[1], detect(ccu[0])))
    rdd = rdd.map(lambda cl: (cl[0], {lang: 1 if lang == cl[1] else 0 for lang in LANGUAGES}))
    rdd = rdd.reduceByKey(lambda a, b: {c: a[c] + b[c] for c in LANGUAGES})

    print(f'For country: {country}, results: {rdd.collect}')


if __name__ == '__main__':
    p = ThreadPool(8)
    p.map(main, COUNTRIES)
    p.close()
    p.join()
