import storage
import test_local_storage


class TestSelectelStorage(test_local_storage.TestLocalStorage):

    def setUp(self):
        self._storage = storage.load("selectel")
