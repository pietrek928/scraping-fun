from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from urllib.parse import urlencode

import requests

from setting import get_headers, get_proxies


class RequestHandler:
    def __init__(self, headers=(), proxies=()):
        self._headers = dict(headers) or get_headers()
        self._proxies = dict(proxies) or get_proxies()

    def get(self, url, params=()):
        try:
            if params:
                url += '?' + urlencode(dict(params))
            r = requests.get(url, headers=self._headers, proxies=self._proxies)
        except Exception as e:
            return f'error: {e.__class__.__name__} {e}'
        return r.text

    def post(self, url, data):
        try:
            if isinstance(data, (str, bytes)):
                r = requests.post(url, data=data, headers=self._headers, proxies=self._proxies)
            else:
                r = requests.post(url, json=data, headers=self._headers, proxies=self._proxies)
        except Exception as e:
            return f'error: {e.__class__.__name__} {e}'
        return r.text


def _get_process_item(req: RequestHandler, url_template, item):
    url = url_template.format(*item.split('#'))
    body = req.get(url)
    return item, body


def map_scrape_get(url_template, items, threads=4):
    req = RequestHandler()
    with ThreadPoolExecutor(threads) as pool:
        yield from pool.map(
            partial(_get_process_item, req, url_template),
            items
        )
