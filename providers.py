from json import JSONDecodeError

from tqdm import tqdm

from scrape import *
from storage import *


def process_melissa():
    with open('zip4.json', 'r') as f:
        rr = json.load(f)

    errors = 0
    with JSONStorage('j') as stor:
        # stor.clear_errors()
        for n, v in map_scrape_get(
                'https://www.melissa.com/v2/lookups/zip4/zip4/?zip4={}&tbl=mak&fmt=json',
                stor.filter_items(tqdm(rr))
        ):
            if ' ' not in n:
                try:
                    v = json.loads(v)
                    stor.store_item(n, v)
                    errors = 0
                except JSONDecodeError:
                    print(n)
                    if '<a href="/v2/lookups/zip4/zip4/' in v:
                        stor.mark_err(n)
                    errors += 1
                    if errors > 32:
                        break
