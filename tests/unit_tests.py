import unittest
import time
import os
import copy
import json
from mock import patch, MagicMock
from datetime import datetime, timedelta

from pymicrodb import PyMicroDB
from pymicrodb.configs import configs
from tests.test_utils import load_sample_doc


class UnitTestCase(unittest.TestCase):
    def setUp(self):
        self.db_path = self._get_db_path()
        self.db = self._init_db(self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def _get_db_path(self):
        return 'DB_TEST_' + str(int(time.time())) + '.bson'

    def _init_db(self, db_path):
        return PyMicroDB(db_path)

    def test_init(self):
        self.assertTrue(self.db.opened)
        self.assertEqual(self.db.db_path, self.db_path)
        # Storage should be empty for a new DB
        self.assertEqual(self.db._storage, {})
        self.assertIsNotNone(self.db._last_saved)
        self.assertIsNotNone(self.db._file_handle)
        self.assertIsNone(self.db._worker)

    def test_del(self):
        db_path = self._get_db_path()
        db = self._init_db(db_path)

        with patch('pymicrodb.PyMicroDB.exit') as exit_func:
            del db

        exit_func.assert_called_once()

    def test_save_worker(self):
        with patch('threading.Thread') as thread_class:
            self.db._last_saved = datetime.now()
            self.db._save_worker()
            # Should not run the save worker when just initialized.
            thread_class.assert_not_called()

            # Should save if exceeded the save interval
            self.db._last_saved = datetime.now() - timedelta(seconds=configs['save_interval'])
            self.db._save_worker()
            thread_class.assert_called_once()

    def test_save(self):
        sample_doc = load_sample_doc()
        # Test save to an empty file
        doc = copy.deepcopy(sample_doc)
        doc['id'] = 1
        self.db._storage['1'] = doc
        doc2 = copy.deepcopy(sample_doc)
        doc2['id'] = 2
        self.db._storage['2'] = doc2
        self.db.save()
        self.assertEqual(self.db._storage, json.load(open(self.db_path, 'r')))

        # Save after entries removed
        del self.db._storage['1']
        self.db.save()
        self.assertEqual(self.db._storage, json.load(open(self.db_path, 'r')))

        # Save after entries inserted
        self.db._storage['1'] = doc
        doc3 = copy.deepcopy(sample_doc)
        doc3['id'] = 3
        self.db._storage['3'] = doc3
        self.db.save()
        self.assertEqual(self.db._storage, json.load(open(self.db_path, 'r')))

    def test_save_expired(self):
        self.db._last_saved = datetime.now() - timedelta(seconds=configs['save_interval'])
        self.assertTrue(self.db.save_expired())

        self.db._last_saved = datetime.now() - timedelta(seconds=configs['save_interval'] - 10)
        self.assertFalse(self.db.save_expired())

        self.db._last_saved = datetime.now()
        self.assertFalse(self.db.save_expired())

        self.db._last_saved = datetime.now() + timedelta(seconds=1)
        self.assertFalse(self.db.save_expired())

    def test_count(self):
        self.assertEqual(self.db.count(), 0)

        sample_doc = load_sample_doc()
        doc = copy.deepcopy(sample_doc)
        doc['id'] = 1
        self.db._storage['1'] = doc
        self.assertEqual(self.db.count(), 1)

        doc2 = copy.deepcopy(sample_doc)
        doc2['id'] = 2
        self.db._storage['2'] = doc2
        self.assertEqual(self.db.count(), 2)

        del self.db._storage['1']
        self.assertEqual(self.db.count(), 1)

    def test_get(self):
        sample_doc = load_sample_doc()
        doc = copy.deepcopy(sample_doc)
        doc['id'] = 1
        self.db._storage['1'] = doc
        self.assertEqual(self.db.get('1'), doc)

        doc2 = copy.deepcopy(sample_doc)
        doc2['id'] = 2
        self.db._storage['2'] = doc2
        self.assertEqual(self.db.get('2'), doc2)

    def test_put(self):
        sample_doc = load_sample_doc()
        doc = copy.deepcopy(sample_doc)
        self.db.put('1', doc)
        self.assertEqual(self.db._storage['1'], doc)

        doc2 = copy.deepcopy(sample_doc)
        self.db.put('2', doc2)
        self.assertEqual(self.db._storage['2'], doc2)

        doc3 = copy.deepcopy(sample_doc)
        self.db.put('3', doc3)
        # Should set `id` if not int the input document
        self.assertEqual(self.db._storage['3'], doc3)

        # Test update
        doc3['title'] = 'test3'
        self.db.put('3', doc3)
        self.assertEqual(self.db._storage['3']['title'], 'test3')

        # Test save worker called
        with patch('pymicrodb.database.PyMicroDB._save_worker') as worker:
            self.db.put('3', doc3)
            worker.assert_called_once()

        # Test lock acquired when putting into storage
        _lock = self.db._lock
        self.db._lock = MagicMock()
        self.db.put('3', doc3)
        self.db._lock.__enter__.assert_called_once()
        self.db._lock = _lock

    def test_insert(self):
        sample_doc = load_sample_doc()
        new_id = self.db.insert(sample_doc)
        self.assertIsInstance(new_id, (str, unicode))
        self.assertEqual(self.db._storage[new_id], sample_doc)

    def test_insert_doc_persist(self):
        """Test if the doc is persisted even if original doc is deleted."""
        sample_doc = load_sample_doc()
        new_id = self.db.insert(sample_doc)

        del sample_doc
        self.assertEqual(self.db._storage[new_id], load_sample_doc())
