import os

import requests


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
        print 'Getting urls of cluster %s' % instance
        dg = DomainGetter(prefix, instance, 'clusters/cluster-{}}.idx'.format(instances))
        yield dg.get_urls(pattern)


def save_cluster_files(prefix, instances):
    for instance in instances:
        full_instance = 'CC-MAIN-' + instance
        cfg = ClusterFileSaver(prefix, full_instance, 'clusters/', 'cluster-{}.idx'.format(instance))
        if not cfg.exists:
            print 'Saving file %s' % full_instance
            cfg.save()


def download_files(pattern):
    prefix = 'https://commoncrawl.s3.amazonaws.com/'
    instances = ['2020-24', '2018-22', '2017-22', '2016-22']

    file_urls = get_file_urls(prefix, instances, '{},'.format(pattern))
    



if __name__ == '__main__':
    download_files('fr')
