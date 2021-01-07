import logging

import langdetect

from detect.abstractdetect import AbstractDetector


class LangdetectDetector(AbstractDetector):
    def detect(self, item):
        logging.debug('Longest sentence to detect: %s', item.longest_sentence)
        return langdetect.detect(' '.join(item.longest_sentence))
