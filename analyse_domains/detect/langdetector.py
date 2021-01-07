import logging

import langdetect

from detect.abstractdetect import AbstractDetector


class LangdetectDetector(AbstractDetector):
    def detect(self, item):
        super().detect(item)
        return langdetect.detect(item)
