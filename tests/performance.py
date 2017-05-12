# -*- coding: utf-8 -*-

import time
import unittest
import os
import json
from datetime import datetime

from pymicrodb import PyMicroDB
from pymicrodb.logger import logger_manager
from pymicrodb.constants import SECOND_TO_MICROSECOND

TEST_FILE_NAME = 'DB_TEST_' + str(int(time.time())) + '.bson'
INSERT_TIMES = 10000
logger = logger_manager.get_logger()


class PyMicroDBPerformanceTestCase(unittest.TestCase):
    def setUp(self):
        self._db = PyMicroDB(TEST_FILE_NAME)
        self._load_sample_doc()

    def tearDown(self):
        # os.remove(TEST_FILE_NAME)
        pass

    def _load_sample_doc(self):
        with open(os.path.join(os.path.dirname(__file__),
                               'fixtures/sample_doc.bson')) as f:
            self._sample_doc = json.loads(f.read())

    def test_inserts_per_second(self):
        start_time = datetime.now()

        for i in range(INSERT_TIMES):
            self._db.insert(self._sample_doc)

        end_time = datetime.now()
        logger.info('{} inserts in {} seconds'
                    .format(INSERT_TIMES,
                            (end_time - start_time).microseconds / SECOND_TO_MICROSECOND))
