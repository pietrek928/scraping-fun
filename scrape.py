from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial

import requests

from setting import get_headers, get_proxies


class RequestHandler:
    def __init__(self, headers=(), proxies=()):
        self._headers = dict(headers) or get_headers()
        self._proxies = dict(proxies) or get_proxies()

    def get(self, url):
        r = requests.get(url, headers=self._headers, proxies=self._proxies)
        r.raise_for_status()
        return r


def _get_process_item(req: RequestHandler, url_template, item):
    url = url_template.format(*item.split('#'))
    r = req.get(url)
    r.raise_for_status()
    return item, r.text


def map_scrape_get(url_template, items, threads=4):
    req = RequestHandler()
    with ThreadPoolExecutor(threads) as pool:
        yield from pool.map(
            partial(_get_process_item, req, url_template),
            items
        )
