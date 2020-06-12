from json import JSONDecodeError

from tqdm import tqdm

from scrape import *
from storage import *


def process_us_parcels():
    errors = 0
    with JSONStorage('cumberland') as stor:
        # stor.clear_errors()
        with open('cumberland_47035970102_1.json', 'r') as f:
            rr = json.load(f)
        rr = tuple(
            v.replace('http:', 'https:') for v in rr
        )
        total_len = len(rr)
        mapp = {
            url.split('/')[-1].split('.')[0] : url for url in rr
        }
        rr = tuple(stor.filter_items(mapp.keys()))
        filtered_len = total_len - len(rr)
        counter = tqdm(map_scrape_get(
            '{}',
            (mapp[v] for v in rr), threads=16
        ), total=total_len)
        counter.update(filtered_len)
        for n, v in counter:
            n = n.split('/')[-1].split('.')[0]
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
