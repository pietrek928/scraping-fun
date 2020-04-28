import requests

from setting import get_headers, get_proxies


def map_scrape_get(url_template, items):
    headers = get_headers()
    proxies = get_proxies()
    for item in items:
        r = requests.get(url_template.format(*item.split('#')), headers=headers, proxies=proxies)
        r.raise_for_status()
        yield item, r.text
