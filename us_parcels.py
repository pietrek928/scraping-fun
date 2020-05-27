from json import JSONDecodeError

from tqdm import tqdm

from scrape import *
from storage import *


def process_us_parcels(st, county, n):
    errors = 0
    county_path = f'us/{st}/{county}'
    with JSONStorage(county_path) as stor:
        # stor.clear_errors()
        rr = tuple(map(str, range(1, n)))
        total_len = len(rr)
        rr = tuple(stor.filter_items(rr))
        filtered_len = total_len - len(rr)
        counter = tqdm(map_scrape_get(
            'https://landgrid.com/' + county_path + '/{}.json',
            rr, threads=16
        ))
        counter.update(filtered_len)
        for n, v in counter:
            try:
                v = json.loads(v)
                if 'id' in v:
                    stor.store_item(n, v)
                    errors = 0
                    continue
            except JSONDecodeError:
                print(v)
            if isinstance(v, dict) and 'Not found' in v.get('message', ""):
                stor.mark_notfound(n)
            else:
                print(n, v)
            errors += 1
            if errors > 4096:
                break
