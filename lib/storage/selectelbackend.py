"""
Selectel Cloud Storage backend
See: https://selectel.ru/services/cloud-storage/
https://github.com/go1dshtein/selectel-api
"""

from . import Storage
import cache_lru
import requests
import selectel


def doesnotexists(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 404:
                raise IOError('File does not exists: \'{0}\''.format(args[1]))
            else:
                raise
    return wrapper


class SelectelStorage(Storage):
    def __init__(self, config):
        auth = config.selectel_auth
        key = config.selectel_key
        container_name = config.selectel_container
        self._container = selectel.storage.Container(auth, key, container_name)

    def make_key(self, path):
        "any path should be absolute"

        if not path.startswith('/'):
            return '/%s' % path
        return path

    def get_info(self, path):
        try:
            return self._container.info(self.make_key(path))
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 404:
                raise OSError('File does not exists: \'{0}\''.format(path))
            else:
                raise

    @cache_lru.get
    @doesnotexists
    def get_content(self, path):
        return self._container.get(self.make_key(path))

    @cache_lru.put
    @doesnotexists
    def put_content(self, path, content):
        self._container.put(self.make_key(path), content)

    @cache_lru.put
    @doesnotexists
    def stream_write(self, path, fp):
        self._container.put_stream(self.make_key(path), fp)

    @cache_lru.get
    @doesnotexists
    def stream_read(self, path):
        return self._container.get_stream(self.make_key(path))

    def get_size(self, path):
        return self.get_info(path)['content-length']

    def list_directory(self, path=None):
        if path is None:
            path = "/"

        keys = self._container.list(self.make_key(path)).keys()

        for key in keys:
            yield key

        if not keys:
            raise OSError(
                'Directory is empty or does not exists: \'{0}\''.format(path))

    def exists(self, path):
        try:
            self.get_info(path)
            return True
        except OSError:
            return False

    @cache_lru.remove
    @doesnotexists
    def remove(self, path):
        self._container.remove(self.make_key(path), force=True)
