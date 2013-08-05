#!/usr/bin/env python
# coding: utf-8

import unittest
from libtasks import fs

class FSTests(unittest.TestCase):

    def test_which(self):
        self.assertEquals(fs.which('ls'), '/bin/ls')
        self.assertEquals(fs.which('sa8d0gf7sdf7g87sdf8asd797asd7'), None)
