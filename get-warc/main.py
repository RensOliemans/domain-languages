import logging

import config
from fetch import Fetcher
from item import FilteredItem
from detect import detect
from store import Store

logging.basicConfig(level=config.LOGLEVEL)


def main():
	fetcher = Fetcher(config.URL, config.LIMIT)
	store = Store()

	for i, item in enumerate(fetcher.objects):
		logging.info('url %s: %s', i, item.url)

		item = FilteredItem(item)
		if item.filter_out:
			continue

		language = detect(item)
		store.add(language)

	print(store)


main()
