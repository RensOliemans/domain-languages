import os

import requests

# from pyspark import SparkContext
# from pyspark.sql import SparkSession
#
#
# APPNAME = 'Download CC GZs'
# sc = SparkContext(appName=APPNAME)
# sc.setLogLevel('ERROR')
# spark = SparkSession.builder.appName(APPNAME).getOrCreate()


class DomainGetter:
    def __init__(self, prefix, instance, cluster_filename):
        self.prefix = prefix
        self.instance = instance
        self.cluster_filename = cluster_filename

    def get_urls(self, pattern):
        urls = ('{}cc-index/collections/{}/indexes/{}'
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
        return '{}cc-index/collections/{}/index/cluster.idx'.format(prefix, instance)


def get_file_urls(prefix, instances, pattern):
    save_cluster_files(prefix, instances)

    for instance in instances:
        print('Getting urls from cluster %s with pattern %s' % (instance, pattern))
        dg = DomainGetter(prefix, instance, 'clusters/cluster-{}.idx'.format(instance))
        yield dg.get_urls(pattern)


def save_cluster_files(prefix, instances):
    for instance in instances:
        full_instance = 'CC-MAIN-' + instance
        cfg = ClusterFileSaver(prefix, full_instance, 'clusters/', 'cluster-{}.idx'.format(instance))
        if not cfg.exists:
            print('Saving file %s' % full_instance)
            cfg.save()
        else:
            print('File %s already exists' % full_instance)


def get_gz_files(file_urls):
    for file_url in file_urls:
        parts = file_url.split('/')
        filename = parts[-1]
        instance = parts[-2]
        out_filename = 'gzs/{}--{}'.format(instance, filename)

        file_url = 'CC-MAIN-{}'.format(file_urls)
        with requests.get(file_url, stream=True) as r:
            print('Downloading file %s' % file_url)
            r.raise_for_status()
            with open(out_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        yield out_filename


def download_files(pattern):
    prefix = 'https://commoncrawl.s3.amazonaws.com/'
    instances = ['2020-50']

    print('Getting file urls, possibly downloading cluster files for it')
    file_urls_per_instance = get_file_urls(prefix, instances, '{},'.format(pattern))

    print('Getting gz_files for instance')
    for instance in file_urls_per_instance:
        gz_filenames = get_gz_files(instance)
        print(list(gz_filenames))


if __name__ == '__main__':
    download_files('fr')
