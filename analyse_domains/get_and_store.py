import logging

import config
from fetch import Fetcher
from item import FilteredItem


def _get_filtered_items(objects):
    for i, item in enumerate(objects):
        logging.info('url %s: %s', i, item.url)
        yield FilteredItem(item)


def main():
    fetcher = Fetcher(config.URL, config.LIMIT, source='https://index.commoncrawl.org/CC-MAIN-2020-50-index',
                      warc_url_prefix='https://commoncrawl.s3.amazonaws.com')

    data = [{'url': item.url, 'content': item.to_detect}
            for item in _get_filtered_items(fetcher.objects)
            if not item.filter_out]

    print(f'Storing {len(data)} items for url: {config.URL}')


if __name__ == '__main__':
    main()
