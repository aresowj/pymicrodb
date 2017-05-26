# -*- coding: utf-8 -*-

import time
import unittest
import os
import json
from datetime import datetime

from pymicrodb import PyMicroDB
from pymicrodb.logger import logger_manager
from pymicrodb.constants import SECOND_TO_MICROSECOND
from tests.test_utils import load_sample_doc


TEST_FILE_NAME = 'DB_TEST_' + str(int(time.time())) + '.bson'
INSERT_TIMES = 10000
logger = logger_manager.get_logger()


class PerformanceTestCase(unittest.TestCase):
    def setUp(self):
        self._db = PyMicroDB(TEST_FILE_NAME)
        self._sample_doc = load_sample_doc()

    def tearDown(self):
        if os.path.exists(TEST_FILE_NAME):
            os.remove(TEST_FILE_NAME)

    def test_inserts_per_second(self):
        start_time = datetime.now()

        for i in range(INSERT_TIMES):
            self._db.insert(self._sample_doc)

        end_time = datetime.now()
        seconds = (end_time - start_time).microseconds / SECOND_TO_MICROSECOND
        per_second = int(INSERT_TIMES / seconds)
        logger.info('{} inserts in {} seconds, {} insert per second.'
                    .format(INSERT_TIMES, seconds, per_second))
