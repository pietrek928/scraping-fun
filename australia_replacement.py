import json
from collections import defaultdict
from itertools import product

from scrape import RequestHandler
from storage import JSONStorage


def api_url(action):
    return f'https://www.lmigroup.com/RiskCoachCalculatorsApi/api/{action}'


def get_locations(req: RequestHandler, location_id, country_id):
    resp = req.get(api_url('Location'), {
        'parentId': location_id,
        'countryId': country_id
    })
    return json.loads(resp)


def scrape_locations(country_id):
    req = RequestHandler()
    with JSONStorage(f'lmigroup_{country_id}') as stor:
        st = [0]
        while st:
            n = st.pop()
            print(n)
            key = f'loc_{n}'
            if stor.has_item(key):
                data = stor.read_item(key)
            else:
                data = get_locations(req, n, country_id)
                stor.store_item(key, data)
            for i in data:
                st.append(i['id'])


def get_construction_types(req: RequestHandler, parent_id, is_primary, country_id):
    resp = req.get(api_url('ConstructionType'), {
        'parentId': parent_id,
        'isPrimary': is_primary,
        'countryId': country_id
    })
    return json.loads(resp)


def scrape_construction_types(country_id):
    req = RequestHandler()
    with JSONStorage(f'lmigroup_{country_id}') as stor:
        st = [(0, True), (0, False)]
        while st:
            n, p = st.pop()
            print(n, p)
            key = f'constr_{n}'
            if p:
                key += 'p'
            if stor.has_item(key):
                data = stor.read_item(key)
            else:
                data = get_construction_types(req, n, p, country_id)
                stor.store_item(key, data)
            for i in data:
                st.append((i['id'], p))


def calculate_cost(req, location_id, construction_id):
    data = {
        "locationId": location_id,
        "calculators": [
            {"id": construction_id, "area": 1000}
        ]
    }
    r = req.post(api_url('BuildingCostCalcualtor'), data)
    return json.loads(r)


def scrape_costs(country_id):
    req = RequestHandler()
    with JSONStorage(f'lmigroup_{country_id}') as stor:
        stor.clear_errors()
        locations = []
        constructions = []
        for i in stor.items():
            if i.startswith('loc_'):
                locations.extend(stor.read_item(i))
            if i.startswith('constr_'):
                constructions.extend(stor.read_item(i))
        for location, construction in product(locations, constructions):
            print(location['id'], construction['id'])
            key = f'cost_{location["id"]}_{construction["id"]}'
            if not stor.has_item(key):
                data = calculate_cost(
                    req, location['id'], construction['id']
                )
                data['location'] = location
                data['construction'] = construction
                if 'message' in data:
                    print(data['message'])
                    stor.mark_notfound(key, data['message'])
                else:
                    stor.store_item(key, data)


def show_construction_types(country_id):
    d = []
    ii = {
        0: 'root'
    }
    with JSONStorage(f'lmigroup_{country_id}') as stor:
        for i in stor.items():
            if i.startswith('constr_'):
                data = stor.read_item(i)
                for v in data:
                    if v:
                        d.append((v['id'], v))
                        ii[v['id']] = v['name']
    for i, v in sorted(d):
        print(i, v['name'])
        print('parent: ', ii[v['parentId']])
        print()


def create_csv(country_id):
    con_n = {}
    loc_n = {}
    occ_used = set()
    loc_used = set()

    cost_array = defaultdict(lambda: defaultdict(list))

    with JSONStorage(f'lmigroup_{country_id}') as stor:
        stor.clear_errors()
        for i in stor.items():
            if i.startswith('loc_'):
                for l in stor.read_item(i):
                    loc_n[l['id']] = l['name'].strip()
            if i.startswith('constr_'):
                for c in stor.read_item(i):
                    con_n[c['id']] = c['name'].strip()
            if i.startswith('cost_'):
                cost_data = stor.read_item(i)
                occ_used.add(cost_data['construction']['id'])
                loc_used.add(cost_data['location']['id'])
                cost_array[cost_data['construction']['id']][cost_data['location']['id']] = [
                    cost_data['lowerCost'], cost_data['upperCost']
                ]

    occ_ids = sorted(occ_used)
    loc_ids = sorted(loc_used)

    with open(f'country_{country_id}.csv', 'w') as f:
        fr = ['']
        fr.extend(map(loc_n.get, loc_ids))
        f.write(','.join(fr) + '\n')
        for oid in occ_ids:
            r = [f'"{con_n[oid]}"']
            for lid in loc_ids:
                r.append('"' + '\n'.join(map(str, cost_array[oid][lid])) + '"')
            f.write(','.join(r) + '\n')


def archive_data(country_id):
    with JSONStorage(f'lmigroup_{country_id}') as stor:
        stor.archive()
