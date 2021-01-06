import cdx_toolkit

print('Initialising CC Fetcher')
cdx = cdx_toolkit.CDXFetcher(source='cc')
URL = '*.nl'

def get_objects(url, limit=100):
    for obj in cdx.iter(url, limit=limit, filter=["!~robots.txt", 'mime:text/html']):
        yield obj

"""
print('Fetching items')
items = get_objects(URL, 15)

for i, item in enumerate(items):
    url = item.data['url']
    print("url {}: {}".format(i, url))

    print(item.content)
"""
