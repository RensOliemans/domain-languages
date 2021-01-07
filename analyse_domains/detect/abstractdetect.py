import logging
from abc import ABC, abstractmethod


class AbstractDetector(ABC):
    @abstractmethod
    def detect(self, item):
        logging.info('Text to detect: %s', item)
