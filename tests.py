# -*- coding: utf-8 -*-
import io
import unittest

#------------------------------------------------------------------------------#
# Sack                                                                         #
#------------------------------------------------------------------------------#
from .sack import *
class TestSack (unittest.TestCase):
    def test_Create (self):
        Sack.Create (io.BytesIO (), 32, 10).Flush ()

    def test_Load (self):
        stream = io.BytesIO ()
        Sack.Create (stream, 32, 10).Flush ()
        Sack (stream, 10)

    def test_PushPop (self):
        stream = io.BytesIO ()
        with Sack.Create (stream, 32) as sack:
            d0 = sack.Push (b'some data')
            d1 = sack.Push (b'some large data' * 100)
            d2 = sack.Push (b'test')

        with Sack (stream) as sack:
            self.assertEqual (sack.Get (d0), b'some data')
            self.assertEqual (sack.Pop (d1), b'some large data' * 100)

        with Sack (stream) as sack:
            self.assertEqual (sack.Get (d2), b'test')
            d2_new = sack.Push (b'test' * 10, d2)
            self.assertNotEqual (d2_new, d2)
            self.assertEqual (sack.Push (b'abc', d1), d1)
# vim: nu ft=python columns=120 :
