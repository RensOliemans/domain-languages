from pyspark import SparkContext
from pyspark.sql import SparkSession

appName = 'detect-language'
sc = SparkContext(appName=appName)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.appName(appName).getOrCreate()


COUNTRY = 'fr'
df = spark.read.option('header', 'true').csv(COUNTRY)
