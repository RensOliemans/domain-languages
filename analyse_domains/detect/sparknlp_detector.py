import logging

from pyspark import SparkContext
from pyspark.sql import SparkSession

import sparknlp
from sparknlp.pretrained import PretrainedPipeline

import config
from detect.abstractdetect import AbstractDetector


class SparkNLPDetector(AbstractDetector):
    def __init__(self):
        self._sc = SparkContext(appName=config.APPNAME)
        self._sc.setLogLevel(config.SPARK_LOGLEVEL)
        self._s = SparkSession.builder.appName(config.APPNAME).getOrCreate()

        self._spark = sparknlp.start()

        self._pipeline = PretrainedPipeline(config.PIPELINE[0], lang=config.PIPELINE[1])

    def detect(self, item):
        super().detect(item)
        return self._pipeline.annotate(item)['language'][0]

