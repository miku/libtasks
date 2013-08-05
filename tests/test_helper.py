#!/usr/bin/env python
# coding: utf-8

from libtasks import helper
import unittest
import marcx

class HelperTests(unittest.TestCase):

    def test_normalized_isbns(self):
        record = marcx.FatRecord()
        record.add('020', a='9783486590029')
        self.assertEquals(set(['9783486590029']),
            helper.normalized_isbns(record))