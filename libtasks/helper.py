#!/usr/bin/env python
# coding: utf-8

from libtasks import xisbn
import pandas as pd
import marcx

""" 
This module contains some helpers.
"""

def read_tsv_to_dict(path, sep='\t'):
    """ 
    Convert a *two* column TSV file into a dictionary where
    the first column is the key and the second column is the value.

    Repeated IDs are overwritten, so should there be more than one in the file,
    the last one wins.
    """
    if not os.path.exists(path) or os.path.isdir(path):
        return dict()
    df = pd.read_csv(path, sep=sep, names=('a', 'b'))
    kvals = dict()    
    for row in df.iterrows():
        series = row[1]
        kvals[series.get('a')] = series.get('b')
    return kvals


def normalized_isbns(record, *fieldspecs):
    """
    Return a list of 13-normalized ISBNs found in this given record.
    """
    normalized = set()
    if not fieldspecs:
        fieldspecs = ('020.a', )
    fatrecord = marcx.FatRecord.from_record(record)
    for isbn in fatrecord.itervalues(*fieldspecs):
        normalized.add(xisbn.normalize(isbn, version='13'))
    return normalized




