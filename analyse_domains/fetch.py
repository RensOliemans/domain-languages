import cdx_toolkit

from item import Item


class Fetcher:
    def __init__(self, url, limit=100, source='cc', warc_url_prefix=None):
        self.url = url
        self.limit = limit
        self._cdx = cdx_toolkit.CDXFetcher(source=source, warc_url_prefix=warc_url_prefix)

    @property
    def objects(self):
        for obj in self._cdx.iter(self.url, limit=self.limit, filter=["!~robots.txt", "mime:text/html"]):
            yield Item(obj)


class ManualFetcher:
    def __init__(self, url, limit, filters, source, warc_url_prefix):
        self.url = url
        self._limit = limit
        self._source = source
        self._filters = filters
        self._warc_url_prefix = warc_url_prefix
        self._params = {'limit': self._limit, 'filter': self._filters, 'url': self.url, 'output': 'json', 'page': 0}
