import json
import os
import tarfile
from contextlib import contextmanager
from datetime import datetime
from os import path, unlink
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

    def is_correct(self, name):
        return self.has_item(name) and isinstance(self._status[name], int)

    def filter_items(self, names):
        for name in names:
            if name not in self._status:
                yield name

    def items(self):
        return self._status.keys()

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
        self._status[name] = int(
            datetime.utcnow().strftime('%s')
        )

    def read_item(self, name):
        with open(self._format_path(name), 'r') as f:
            return json.load(f)

    def del_item(self, name):
        unlink(self._format_path(name))

    def mark_err(self, name, msg=''):
        self._status[name] = f'err {msg}'

    def mark_notfound(self, name, msg=''):
        self._status[name] = f'notfound {msg}'

    def archive(self):
        archive_dir = 'archive'
        os.makedirs(archive_dir, exist_ok=True)
        fname = path.join(archive_dir, self._d.replace('/', '_') + '.tar.xz')
        with tarfile.open(fname, "w:xz") as tar:
            tar.add(self._d)
