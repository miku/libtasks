#!/usr/bin/env python
# coding: utf-8

from libtasks import xisbn as isbn
import base64
import bz2
import os
import unittest

# http://www.amazon.de/Algorithmen-Eine-Einf%C3%BChrung-Thomas-Cormen/dp/3486590022
ISBNS_LIKE_3486590022 = (
    '0138134596',
    '3486590022',
    '3897217201',
    '0273751506',
    '3827329949',
    '0596005652',
    '3540763937',
    '3486587234',
    '3898646785',
    '3528665084',
    '3834801267',
    '3499612240',
    '3868941851',
    '3540419233',
    '3540774319',
    '3827410053',
    '354030150X',
    '3540418504',
    '3868940995',
    '0000000000',
    '0262533057',
    '3868941371',
    '0470233990',
    '3540466606',
    '3827428033',
    '9783486590029',
    '0321623215',
    '3827373425',
    '3826691881',
    '3827425255',
    '3827329019',
    '3519221217',
    '3486597116',
    '3446426639',
    '3868940820',
    '3827418240',
    '383621752X',
    '3486598341',
    '3446421505',
    '389864765X',
    '1000502953',
    '3446426396',
    '2829306031',
    '1449327141',
    '3834809969',
    '3898646637',
    '0262033844',
    '3834813052',
    '383481749X',
    '3836214113',
    '3834812579',
    '3642299431',
    '3827428955',
    '3486591908',
    '3836216353',
    '0764574817',
    '3499600749',
    '3836217112',
    '3834805696',
    '3642128939',
    '3893480294',
    '2810289031',
    '1000655583',
    '3486590029',
    '1000482783',
    '2616664031',
    '1375466400',
    '2777963369',
    '1000655923',
    '1056475726',
    '1760236031',
    '1375362409',
    '1000601883',
    '3184302505',
    '1333619031',
    '7304474530',
    '2076361031',
    '1720689031',
    '1000660033',
    '1000655573',
    '4056229786',
    '1966060031',
    '1375362409446',
    '2076254031',
    '3515399030',
    '6052461168',
    '2076820031',
    '1661648031',
    '1375381800',
    '1000655933',
    '1899362992',
    '2454118031',
    '2611181031',
    '1000721273',
    '2624847031',
    '2829305031',
    '2076939031',
    '2076882031',
    '1000683953',
    '1530533615',
)


class ISBNTests(unittest.TestCase):

    def test_searchiter(self):
        candidates = set(isbn.searchiter('123123'))
        self.assertEquals(candidates, set())

        candidates = set(isbn.searchiter('3486590022'))
        self.assertEquals(candidates, set(['3486590022']))

        candidates = list(isbn.searchiter('3486590022'))
        self.assertEquals(candidates, ['3486590022'])

        candidates = list(isbn.searchiter('3486590022  978-3486590029'))
        self.assertEquals(candidates, ['9783486590029', '3486590022', '3486590029'])

        candidates = list(isbn.searchiter('3486590022 978-3486590029 3486590022'))
        self.assertEquals(candidates, ['9783486590029', '3486590022', '3486590029'])

        with open(os.path.join(
            os.path.dirname(__file__), '3486590022.b64.txt')) as handle:
            s = handle.read()
            candidates = set(isbn.searchiter(bz2.decompress(base64.b64decode(s))))
            self.assertEquals(candidates, set(ISBNS_LIKE_3486590022))

    def test_searchiter_invalid_isbn(self):
        """ search yields ISBN candidates, not valid isbns """
        candidates = list(isbn.searchiter('3486590021'))
        self.assertEquals(candidates, ['3486590021'])

    def test_searchiter_with_dashes(self):
        candidates = list(isbn.searchiter('348-6590022'))
        self.assertEquals(candidates, ['3486590022'])

        candidates = list(isbn.searchiter('348-65900-22'))
        self.assertEquals(candidates, ['3486590022'])

        candidates = list(isbn.searchiter('348-659-0022'))
        self.assertEquals(candidates, ['3486590022'])

        candidates = list(isbn.searchiter('348-65---90--022'))
        self.assertEquals(candidates, ['3486590022'])

        candidates = list(isbn.searchiter('348-65---90--02-2-'))
        self.assertEquals(candidates, ['3486590022'])

    def test_searchiter_with_spurious_chars(self):
        candidates = list(isbn.searchiter('348.6590022'))
        self.assertEquals(candidates, [])

    def test_searchiter_with_whitespace(self):
        """ whitespace is not handled """
        candidates = list(isbn.searchiter('348-65---90--02 2'))
        self.assertEquals(candidates, [])

    def test_searchiter_multiple_matches(self):
        """ whitespace is not handled """
        candidates = list(isbn.searchiter('hello 0262533057 asd 0-262533058'))
        self.assertEquals(candidates, ['0262533057', '0262533058'])

    def test_searchiter_html(self):
        with open(os.path.join(
            os.path.dirname(__file__), '0000545771.html')) as handle:
            s = handle.read()
            candidates = set(isbn.searchiter(s))
            self.assertEquals(candidates, set(['0000545771', '3486590022', 
                '0018221827', '0019521724', '9783486590029']))

    def test_filtervalid_html(self):
        with open(os.path.join(
            os.path.dirname(__file__), '0000545771.html')) as handle:
            s = handle.read()
            candidates = set(isbn.searchiter(s))
            real = set(isbn.filtervalid(candidates))
            self.assertEquals(real, set(['3486590022', '9783486590029']))

    def test_normalize(self):
        with open(os.path.join(
            os.path.dirname(__file__), '0000545771.html')) as handle:
            s = handle.read()
            candidates = set(isbn.searchiter(s))
            real = set(isbn.filtervalid(candidates))
            normalized = set(isbn.normalizeiter(real))
            self.assertEquals(normalized, set(['9783486590029']))
            self.assertEquals(set(isbn.normalizeiter(('123-123-123-1',))), 
                              set(['9781231231234']))

    def test_normalize_fails(self):
        with self.assertRaises(ValueError):
            # set(...) is necessary here
            set(isbn.normalizeiter(set('123', )))

        with self.assertRaises(ValueError):
            set(isbn.normalizeiter(('12312312311',)))

        with self.assertRaises(ValueError):
            set(isbn.normalizeiter(('1-123-123-123-1',)))

