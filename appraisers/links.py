from typing import Iterable

from lxml.html import parse


def find_links(fname) -> Iterable[str]:
    tree = parse(fname)
    r = tree.getroot()
