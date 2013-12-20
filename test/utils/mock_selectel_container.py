
'''Monkeypatch selectel container for unittesting'''

import cStringIO

import requests
import selectel.storage

import mock_dict
import utils


class Error404(requests.exceptions.HTTPError):
    def __init__(self, *args, **kwargs):
        class Response(object):
            status_code = 404
        super(Error404, self).__init__(*args, **kwargs)
        self.response = Response()


def doesnotexists(func):
    def wrapper(this, path):
        if path not in Container._container:
            raise Error404("File does not exist")
        return func(this, path)
    return wrapper


class Container(selectel.storage.Container):
    __metaclass__ = utils.monkeypatch_class

    _container = mock_dict.MockDict()
    _container.add_dict_methods()

    def __init__(self, *args, **kwargs):
        pass

    @doesnotexists
    def get(self, path):
        return Container._container[path]

    def put(self, path, content):
        Container._container[path] = content

    @doesnotexists
    def get_stream(self, path):
        chunk = 2 ** 20
        value = Container._container[path]
        return (value[i:i + chunk] for i in range(0, len(value), chunk))

    def put_stream(self, path, descriptor):
        stream = cStringIO.StringIO()
        stream.write(descriptor.read())
        Container._container[path] = stream.getvalue()

    def remove(self, path, force=True):
        if path in Container._container:
            del Container._container[path]

    @doesnotexists
    def info(self, path):
        return {"content-length": len(Container._container[path])}

    def list(self, path):
        keys = filter(lambda x: x.startswith(path),
                      Container._container.viewkeys())
        return {key: None for key in keys}
