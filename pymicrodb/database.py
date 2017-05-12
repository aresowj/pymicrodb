# -*- coding: utf-8 -*-

import json
import threading
import os
import time
from datetime import datetime

from pymicrodb.logger import logger_manager
from pymicrodb.utils import generate_id
from pymicrodb.configs import configs

logger = logger_manager.get_logger()


class PyMicroDB(object):
    def __init__(self, db_path):
        # type: (str) -> None
        self._db_path = db_path
        self.opened = False
        self._storage = {}  # type: dict[str, dict]
        self._save_lock = threading.RLock()
        self._last_saved = datetime.now()
        self._open()
        self._load()
        self._start_worker()

    def __del__(self):
        self.exit()

    def _open(self):
        self._file_handle = open(self._db_path, 'w+b')
        self.opened = True

    def _load(self):
        # type: () -> None
        try:
            self._storage = json.load(self._file_handle)
        except ValueError:
            self._storage = {}

    def close(self):
        self._file_handle.truncate()
        self._file_handle.close()

    def exit(self):
        if self.opened:
            self.save()
            self.close()

    def _start_worker(self):
        self._worker = threading.Thread(target=self._save_worker)
        self._worker.setDaemon(True)
        self._worker.start()

    def _save_worker(self):
        """Save in-memory storage into a local file by interval."""
        while True:
            time.sleep(configs['save_interval'])
            self.save()

    def save(self):
        logger.debug('Saving entries to local file {}...'.format(self._db_path))
        with self._save_lock:
            self._file_handle.seek(0)
            json.dump(self._storage, self._file_handle)

    def save_expired(self):
        """Decide if current data needs to be saved due to save time expired."""
        return (datetime.now() - self._last_saved).seconds >= configs['save_interval']

    def count(self):
        return len(self._storage)

    def get(self, key):
        # type: (str) -> dict
        return self._storage.get(key)

    def put(self, key, doc):
        # type: (str, dict) -> None
        doc['id'] = key
        self._storage[key] = doc

    def insert(self, doc):
        # type: (dict) -> str
        new_id = generate_id()
        self.put(new_id, doc)

        return new_id
