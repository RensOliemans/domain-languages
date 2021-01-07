import logging

import config
from fetch import Fetcher
from item import FilteredItem
from detect import Detector
from store import Store

logging.basicConfig(level=config.LOGLEVEL)


def main():
	fetcher = Fetcher(config.URL, config.LIMIT)
	detector = Detector()
	store = Store()

	for i, item in enumerate(fetcher.objects):
		logging.info('url %s: %s', i, item.url)

		item = FilteredItem(item)
		if item.filter_out:
			continue

		language = detector.detect(item.to_detect)
		logging.info('Language: %s', language)
		store.add(language)

	print(store)


main()
