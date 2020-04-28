import json
import os
from contextlib import contextmanager
from datetime import datetime
from os.path import isfile


@contextmanager
def preserve_status(fname, data_creator=dict):
    try:
        with open(fname, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        data = data_creator()

    try:
        yield data
    finally:
        with open(fname, 'w') as f:
            json.dump(data, f)


class JSONStorage:
    def __init__(self, d):
        self._d = d

    def __enter__(self):
        os.makedirs(self._d, exist_ok=True)
        self._status_mgr = preserve_status(f'{self._d}/status.json')
        self._status = self._status_mgr.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._status_mgr.__exit__(exc_type, exc_val, exc_tb)

    def has_item(self, name):
        return name in self._status

    def filter_items(self, names):
        for name in names:
            if name not in self._status:
                yield name

    def _format_path(self, name):
        return f'{self._d}/{name}.json'

    def clear_errors(self):
        for name, v in tuple(self._status.items()):
            if (
                    isinstance(v, str) and v.startswith('err')
            ) or not isfile(self._format_path(name)):
                del self._status[name]

    def store_item(self, name, json_data):
        with open(self._format_path(name), 'w') as f:
            json.dump(json_data, f)
        self._status[name] = int(datetime.utcnow().strftime('%s'))

    def mark_err(self, name, msg=''):
        self._status[name] = f'err {msg}'
