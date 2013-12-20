import storage
import test_local_storage

from utils.mock_selectel_container import Container  # noqa


class TestSelectelStorage(test_local_storage.TestLocalStorage):

    def setUp(self):
        self._storage = storage.load("selectel")
