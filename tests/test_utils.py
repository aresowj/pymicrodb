"""Helpers for testing."""
import os
import json


def load_sample_doc():
    with open(os.path.join(os.path.dirname(__file__),
                           'fixtures/sample_doc.bson')) as f:
        return json.loads(f.read())
