import os
import requests

"""
This module downloads index files (gzip-compressed) from the CC index server,
so we have the entire index server locally and can use that to extract WARC
locations much more quickly
"""


def main(languages, instances):
    for language in languages:
        # Download the .gz files. These will be stored on disk which can later be used to download the WARC files
        download_files(language, instances)


# ------------------------  DOWNLOADING .GZ FILES -----------------------
def download_files(pattern, instances):
    prefix = 'https://commoncrawl.s3.amazonaws.com/'

    print('Getting file urls, possibly downloading cluster files for it')
    file_urls_per_instance = get_file_urls(prefix, instances, '{},'.format(pattern))

    print('Getting gz_files for instance')
    for file_urls in file_urls_per_instance:
        # Each file url is a url where to download a .gz file
        gz_filenames = get_gz_files(file_urls)
        print(list(gz_filenames))


def get_file_urls(prefix, instances, pattern):
    """
    This saves cluster.idx files (gets them from index.commoncrawl.org) and then
    finds what parts of the index belong to a given pattern. Example:

    'fr' is covered in the cluster in files: 00190.gz, 00191.gz, 00192.gz, 00193.gz and 00194.gz
    This method then returns something like
    [https://commoncrawl.s3.amazonaws.com/cc-index/collection/CC-MAIN-2020-50/indexes/00190.gz,
     https://commoncrawl.s3.amazonaws.com/cc-index/collection/CC-MAIN-2020-50/indexes/00191.gz,
     etc]
    which are ready to download and store on disk

    """
    save_cluster_files(prefix, instances)  # cluster.idx

    for instance in instances:
        print('Getting urls from cluster %s with pattern %s' % (instance, pattern))
        dg = DomainGetter(prefix, instance, 'clusters/cluster-{}.idx'.format(instance))
        yield dg.get_urls(pattern)


def save_cluster_files(prefix, instances):
    """
    This method takes a lot of cluster.idx files (~200MB each, these are a 'summary' of an index server).
    For each cluster.idx file, it downloads them and stores them if they don't exist yet,
    and does nothing if they do exit
    """
    for instance in instances:
        full_instance = 'CC-MAIN-' + instance
        cfg = ClusterFileSaver(prefix, full_instance, 'clusters/', 'cluster-{}.idx'.format(instance))
        if not cfg.exists:
            print('Saving file %s' % full_instance)
            cfg.save()
        else:
            print('File %s already exists' % full_instance)


def get_gz_files(file_urls):
    """
    This downloads a .gz file from a given url and stores it. For example, if you give
     'https://commoncrawl.s3.amazonaws.com/.../CC-MAIN-2020-50/.../00190.gz,
    this file stores the file on disk and returns the location of where it was stored.
    """
    for file_url in file_urls:
        parts = file_url.split('/')
        filename = parts[-1]
        instance = parts[-3]
        out_filename = 'gzs/{}--{}'.format(instance, filename)

        if os.path.isfile(out_filename):
            print('File %s already exists' % out_filename)
            yield out_filename
            continue

        with requests.get(file_url, stream=True) as r:
            print('Downloading file %s' % file_url)
            r.raise_for_status()
            with open(out_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        yield out_filename


class DomainGetter:
    def __init__(self, prefix, instance, cluster_filename):
        self.prefix = prefix
        self.instance = instance
        self.cluster_filename = cluster_filename

    def get_urls(self, pattern):
        urls = ('{}cc-index/collections/CC-MAIN-{}/indexes/{}'
                .format(self.prefix, self.instance, f)
                for f in self._get_filenames(pattern))
        for url in urls:
            yield url

    def _get_filenames(self, pattern):
        with open(self.cluster_filename, 'r') as f:
            return sorted(list({self._get_filename(line) for line in f if line.startswith(pattern)}))

    @staticmethod
    def _get_filename(line):
        return line.split('\t')[1]


class ClusterFileSaver:
    def __init__(self, prefix, instance, location, filename):
        self.url = self.get_url(prefix, instance)
        self.file_location = location + filename

    def save(self):
        with requests.get(self.url, stream=True) as r:
            r.raise_for_status()
            with open(self.file_location, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return self.file_location

    @property
    def exists(self):
        return os.path.isfile(self.file_location)

    @staticmethod
    def get_url(prefix, instance):
        return '{}cc-index/collections/{}/indexes/cluster.idx'.format(prefix, instance)


main(['se', 'it', 'es', 'ru', 'gr', 'de', 'uk', 'fr'], ['2017-47'])

