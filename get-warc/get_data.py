import cdx_toolkit


class Fetcher:
    def __init__(self, url, limit=100, source='cc'):
        self.url = url
        self.limit = limit
        self._cdx = cdx_toolkit.CDXFetcher(source=source)

    @property
    def objects(self):
        for obj in self._cdx.iter(self.url, limit=self.limit, filter=["!~robots.txt", "mime:text/html"]):
            yield obj
