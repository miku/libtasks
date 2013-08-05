#!/usr/bin/env python
# coding: utf-8

"""
ISBN related functions. 

"""

import collections
import re
import pyisbn


def searchiter(s):
    """
    Search for ISBN-like strings in `s`. This will yield non ISBNs and 
    doubles.
    """
    for regexp in [r'(?:[^0-9]|^| )((?:[0-9]-*){12}[0-9X])(?:[^0-9X]|$)',
                   r'(?:[^0-9]|^| )((?:[0-9]-*){9}[0-9X])(?:[^0-9X]|$)']:
        for match in re.finditer(regexp, s):
            candidate = match.group(1)
            yield candidate.replace("-", "")


def search(s):
    """ Non generator version """
    return set(searchiter(s))


def filtervalid(iterable):
    """ pick out the valid isbns from a set """
    for element in iterable:
        if pyisbn.Isbn(element.strip()).validate():
            yield element


def normalize(element, version='13'):
    element = element.replace('-', '')
    if version == '13':
        if len(element) == 10:
            return pyisbn.convert(element)
        elif len(element) == 13:
            return pyisbn.convert(pyisbn.convert(element))
        else:
            raise ValueError('ISBN is neither 10 or 13 chars long')
    elif version == '10':
        if len(element) == 10:
            return pyisbn.convert(pyisbn.convert(element))
        elif len(element) == 13:
            return pyisbn.Isbn13(element).convert()
        else:
            raise ValueError('ISBN is neither 10 or 13 chars long')
    else:
        raise ValueError('version can only be one of 10 or 13')


def normalizeiter(iterable, version='13'):
    """ return all isbns written in one version """
    for element in iterable:
        yield normalize(element)
