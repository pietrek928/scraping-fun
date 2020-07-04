from collections import defaultdict
from dataclasses import dataclass
from itertools import chain
from typing import Iterable, Tuple, Union

from lxml.html import HtmlElement, parse


class Proposer:
    search_pattern: str = ''

    def find_proposals(self, e: HtmlElement) -> Iterable[Tuple[str, str]]:
        return ()


class TableProposer(Proposer):
    search_pattern: str = '//table'

    @staticmethod
    def _td_text(e: HtmlElement):
        tt = sorted(
            (t.strip(' :\n\r') for t in e.xpath(".//text()")),
            key=lambda t: len(t),
            reverse=True
        )
        if len(tt) > 7 or not tt:
            return ''
        return ''.join(tt)

    @classmethod
    def _match_rows(cls, rowh, rowv):
        h = list(reversed(rowh))
        v = list(reversed(rowv))
        hn = 0
        vn = 0
        hh = None
        vv = None

        while True:
            if not hn:
                if not h:
                    break
                hh = h.pop()
                hn = int(hh.attrib.get('colspan', 1))
            if not vn:
                if not v:
                    break
                vv = v.pop()
                vn = int(hh.attrib.get('colspan', 1))
            hn -= 1
            vn -= 1
            th = cls._td_text(hh)
            tv = cls._td_text(vv)
            if th and tv:
                yield th, tv

    @staticmethod
    def _tr_cells(tr: HtmlElement):
        return list(tr.xpath('./td | ./th'))

    @staticmethod
    def _table_trs(table: HtmlElement):
        # tbody = table.find('tbody')
        # if tbody is not None:
        #     print('aaaaaaaaaa')
        #     table = tbody
        return tuple(tuple(table.findall('tr')))


class TableVerticalProposer(TableProposer):
    def find_proposals(self, e: HtmlElement) -> Iterable[Tuple[str, str]]:
        last_row = []
        for tr in self._table_trs(e):
            current_row = self._tr_cells(tr)
            yield from self._match_rows(last_row, current_row)
            last_row = current_row


class TableHorizontalProposer(TableProposer):
    def find_proposals(self, e: HtmlElement) -> Iterable[Tuple[str, str]]:
        for tr in self._table_trs(e):
            current_row = self._tr_cells(tr)
            if current_row:
                last_td = current_row[0]
                for td in current_row[1:]:
                    yield self._td_text(last_td), self._td_text(td)
                    last_td = td


class TableVerticalHeaderProposer(TableProposer):
    def find_proposals(self, e: HtmlElement) -> Iterable[Tuple[str, str]]:
        trs = self._table_trs(e)
        if trs:
            header_row = self._tr_cells(trs[0])
            for i, tr in enumerate(trs[1:]):
                current_row = self._tr_cells(tr)
                for k, v in self._match_rows(header_row, current_row):
                    yield f'{k}:{str(i).zfill(3)}', v


PROPOSERS: Tuple[Proposer, ...] = (
    TableVerticalProposer(),
    TableVerticalHeaderProposer(),
    TableHorizontalProposer()
)


@dataclass
class PropertyProposal:
    key: str = ''
    value: Union[str, Tuple[str]] = ''
    proposer: Proposer = None
    path: str = '/'


def find_table_proposals(proposals):
    cols = defaultdict(list)

    for k, v in proposals:
        if ':' in k:
            try:
                n = int(k.split(':')[-1])
                k = ':'.join(k.split(':')[:-1])
            except ValueError:
                continue
            cols[k].append((n, v))

    for k, vals in cols.items():
        col = [None] * (max(vals)[0] + 1)
        for n, v in vals:
            col[n] = v
        yield k, tuple(col)


def parse_file(fname) -> Iterable[PropertyProposal]:
    tree = parse(fname)
    r = tree.getroot()
    for proposer in PROPOSERS:
        for e in r.xpath(proposer.search_pattern):
            current_path = tree.getpath(e)
            proposals = tuple(proposer.find_proposals(e))

            for k, v in chain(
                    find_table_proposals(proposals), proposals
            ):
                yield PropertyProposal(
                    key=k,
                    value=v,
                    proposer=proposer,
                    path=current_path
                )
