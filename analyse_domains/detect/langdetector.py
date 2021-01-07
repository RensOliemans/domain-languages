import logging

import langdetect

from detect.abstractdetect import AbstractDetector


class LangdetectDetector(AbstractDetector):
    def detect(self, item):
        logging.debug('Text to detect: %s', item)
        return langdetect.detect(' '.join(item))
