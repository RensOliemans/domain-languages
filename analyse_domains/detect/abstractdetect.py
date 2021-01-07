import logging
from abc import ABC, abstractmethod

import langdetect


class AbstractDetector(ABC):
    @abstractmethod
    def detect(self, item):
        pass
