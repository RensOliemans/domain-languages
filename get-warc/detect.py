import logging

import langdetect


def detect(item):
    logging.debug('Longest sentence to detect: %s', item.longest_sentence)
    return langdetect.detect(' '.join(item.longest_sentence))
