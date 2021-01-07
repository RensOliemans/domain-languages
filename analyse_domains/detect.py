import logging
from abc import ABC, abstractmethod

import langdetect


class AbstractDetector(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def detect(self, item):
        pass


class LangdetectDetector(AbstractDetector):
    def detect(self, item):
        logging.debug('Longest sentence to detect: %s', item.longest_sentence)
        return langdetect.detect(' '.join(item.longest_sentence))
