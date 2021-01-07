import logging

logging.basicConfig(level=logging.INFO)

URL = '*.fr'
LIMIT = 50
MIN_AMOUNT = 5
MIN_LENGTH = 100
APPNAME = 'NLP'
SPARK_LOGLEVEL = "ERROR"
PIPELINE = ('detect_language_20', 'xx')
