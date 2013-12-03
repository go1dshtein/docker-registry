import os
import storage
import test_local_storage
import unittest


@unittest.skipIf(os.environ.get("SELECTEL_CONTAINER") is None,
                 "required specific environment variables for selectel")
class TestSelectelStorage(test_local_storage.TestLocalStorage):

    def setUp(self):
        self._storage = storage.load("selectel")
