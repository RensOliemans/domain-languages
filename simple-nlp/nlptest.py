from pyspark import SparkContext
from pyspark.sql import SparkSession

import sparknlp
from sparknlp.pretrained import PretrainedPipeline

APPNAME = 'nlptest'
sc = SparkContext(appName=APPNAME)
sc.setLogLevel("ERROR")
s = SparkSession.builder.appName(APPNAME).getOrCreate()

spark = sparknlp.start()

pipeline = PretrainedPipeline('detect_language_20', lang='xx')
result = pipeline.annotate("Differences between the master and Vellert include the paintings' 'depiction of tensely gesturing hands, often with palms turned outward and the fingers splayed', as in the Lille and Rotterdam pictures, a 'trademark'. The paintings show a 'predeliction' for 'grotesque, contorted heads', as in the kneeling Magus in Rotterdam. Other traits are a liking for a low viewpoint, small pieces of trompe-l'œil, and elements projecting over a ledge, such as the kneeling shepherd's foot at Lille")

print(result)
