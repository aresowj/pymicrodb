# -*- coding: utf-8 -*-

import json
import threading
import os

from pymicrodb.logger import logger_manager
from pymicrodb.utils import generate_id


logger = logger_manager.get_logger()


class Database(object):
    def __init__(self, db_path):
        # type: (str) -> None
        self._db_path = db_path
        self._storage = {}  # type: dict[str, dict]
        self._file = None
        self._write_lock = threading.RLock()
        self._save_lock = threading.RLock()
        self._load()

    def _load(self):
        # type: () -> None
        if not os.path.exists(self._db_path):
            return

        with open(self._db_path, 'rb') as f:
            line_no = 1
            for line in f.readlines():
                if not line:
                    continue

                try:
                    doc = json.loads(line.decode(encoding='utf-8'))
                except (TypeError, UnicodeDecodeError) as e:
                    logger.error('Failed to decode document at line {}'.format(line_no))
                    logger.exception(e)
                else:
                    try:
                        self._storage[doc['id']] = doc
                    except KeyError:
                        logger.warning('Field `id` not found for document at line {}'.format(line_no))

                line_no += 1

    def _save(self):
        logger.info('Saving entries to local file {}...'.format(self._db_path))
        with self._save_lock:
            with open(self._db_path, 'wb') as f:
                for doc in self._storage.values():
                    f.write((json.dumps(doc) + '\n').encode(encoding='utf-8'))

    def get(self, key):
        # type: (str) -> dict
        return self._storage.get(key)

    def put(self, key, doc):
        # type: (str, dict) -> None
        doc['id'] = key
        self._storage[key] = doc

        with self._write_lock:
            threading.Thread(target=self._save).run()

    def insert(self, doc):
        # type: (dict) -> str
        new_id = generate_id()
        doc['id'] = new_id

        self.put(new_id, doc)

        return new_id
