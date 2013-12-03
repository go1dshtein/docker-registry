import cache
from . import Storage

from selectel.storage import Container


def ioerror(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as err:
            raise IOError(err)
    return wrapper


class SelectelStorage(Storage):
    def __init__(self, config):
        auth = config.selectel_auth
        key = config.selectel_key
        container_name = config.selectel_container
        self._container = Container(auth, key, container_name)

    def make_key(self, path):
        if not path.startswith('/'):
            return '/%s' % path
        return path

    @cache.get
    @ioerror
    def get_content(self, path):
        return self._container.get(self.make_key(path))

    @cache.put
    @ioerror
    def put_content(self, path, content):
        self._container.put(self.make_key(path), content)

    @cache.put
    @ioerror
    def stream_write(self, path, fp):
        self._container.put_stream(self.make_key(path), fp)

    @cache.get
    @ioerror
    def stream_read(self, path):
        return self._container.get_stream(self.make_key(path))

    @ioerror
    def get_size(self, path):
        return self._container.info(self.make_key(path))['content-length']

    @ioerror
    def list_directory(self, path=None):
        if path is None:
            path = "/"
        return self._container.list(self.make_key(path)).keys()

    def exists(self, path):
        try:
            self._container.info(self.make_key(path))
            return True
        except Exception as err:
            return False

    @cache.remove
    @ioerror
    def remove(self, path):
        self._container.remove(self.make_key(path), force=True)
