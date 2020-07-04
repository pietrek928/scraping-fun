from collections import defaultdict
from itertools import chain
from operator import itemgetter
from typing import Iterable

from lxml.html import parse

from appraisers.page import PropertyProposal, find_table_proposals, parse_file


def _number_rate(s: str):
    if not s:
        return 0.
    return sum(c.isdigit() for c in s) / len(s)


class ValueConverter:
    weight = 1.

    def convert(self, v: str):
        return v

    def validate(self, v: str) -> float:
        try:
            self.convert(v)
            return 1.
        except Exception:
            # print(self.__class__, v)
            return 0.


class AddressConverter(ValueConverter):
    weight = 1.05

    def validate(self, v: str) -> float:
        r = super().validate(v)
        nums = _number_rate(v)
        r *= float(.05 <= nums <= .35)
        return r


class IntConverter(ValueConverter):
    weight = 1.05

    def convert(self, v: str):
        return int(v)


class FloatConverter(ValueConverter):
    weight = 1.01

    def convert(self, v: str):
        return float(v)


class MoneyConverter(FloatConverter):
    weight = 1.1

    def convert(self, v: str):
        if not v.startswith('$'):
            raise ValueError('No dollar sign')
        v = v.strip(' $').replace(',', '')
        return super().convert(v)


class ColumnConverter:
    def __init__(self, cell_converter: ValueConverter):
        self.cell_converter = cell_converter
        self.weight = cell_converter.weight * 1.2

    def validate(self, vs: Iterable[str]):
        vs = tuple(vs)
        if not vs:
            return 0.

        s = 0.
        for v in vs:
            s += self.cell_converter.validate(v)
        return s / len(vs)

    def convert(self, vs: Iterable[str]):
        vs = tuple(vs)
        r = []
        for v in vs:
            try:
                r.append(self.cell_converter.convert(v))
            except Exception:
                r.append(None)
        return tuple(r)


CONVERTERS = (
    IntConverter(),
    FloatConverter(),
    MoneyConverter(),
    AddressConverter(),
)

TABLE_CONVERTERS = (
    ColumnConverter(IntConverter()),
    ColumnConverter(FloatConverter()),
    ColumnConverter(MoneyConverter())
)


def score_props(props: Iterable[PropertyProposal]):
    scores = defaultdict(float)
    for p in props:
        if not p.key:
            continue
        if _number_rate(p.key) > .5:
            continue

        if isinstance(p.value, str):
            cvts = CONVERTERS
        else:
            cvts = TABLE_CONVERTERS
        for c in cvts:
            key = (p.key, p.path, p.proposer, c)
            scores[key] += c.validate(p.value) * c.weight
    return sorted(scores.items(), key=lambda v: v[1], reverse=True)


def filter_converters(files):
    n = len(files)
    scores = score_props(chain(*(
        parse_file(f) for f in files
    )))
    used = set()
    for k, s in scores:
        key, path, proposer, cvt = k
        if n * .1 <= s <= 2.5 * n:
            ku = (path, key)
            if ':' in key:
                if (path, key.split(':')[0]) in used:
                    continue
            if ku not in used:
                used.add(ku)
                yield k


def _format_converted(v):
    if v is None:
        return ''
    if isinstance(v, tuple):
        return '\n'.join(
            map(_format_converted, v)
        )
    return str(v)


def show_csv(files):
    cvts = tuple(
        filter_converters(files)
    )

    path_cvts = defaultdict(dict)
    for key, path, proposer, cvt in cvts:
        path_cvts[(path, proposer)][key] = cvt
    hdr = tuple(sorted(
        (path, key) for key, path, proposer, cvt in cvts
    ))
    yield tuple(
        map(itemgetter(1), hdr)
    )
    for f in files:
        vals = defaultdict(str)
        tree = parse(f)
        r = tree.getroot()
        for (path, proposer), val_cvts in path_cvts.items():
            ee = r.xpath(path)
            if ee:
                proposals = tuple(proposer.find_proposals(ee[0]))
                for k, v in chain(
                        find_table_proposals(proposals), proposals
                ):
                    if k in val_cvts:
                        try:
                            vals[(path, k)] = _format_converted(val_cvts[k].convert(v))
                        except ValueError:
                            pass
        yield tuple((
            vals[key] for key in hdr
        ))
