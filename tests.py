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

#------------------------------------------------------------------------------#
# B+Tree                                                                       #
#------------------------------------------------------------------------------#
from .bptree import *
from .providers.simple import *
from random import shuffle

class BPTreeTest (unittest.TestCase):
    def test_Compare (self):
        provider = self.provider ()
        tree, std = BPTree (provider), {}

        # compare tree and map mappings
        def validate (tree):
            self.assertEqual (len (tree), len (std))
            for k, v in tree.items ():
                self.assertEqual (std.get (k), v)
            for k, v in std.items ():
                self.assertEqual (tree.get (k), v)

        # item count
        count = 1 << 10

        # reload
        provider = self.provider (provider)
        tree = BPTree (provider)
        validate (tree)

        # Insertion (10 .. 1024)
        for i in range (10, count):
            tree [i], std [i] = str (i), str (i)
        validate (tree)

        # reload
        provider = self.provider (provider)
        tree = BPTree (provider)
        validate (tree)

        # Insert (0 .. 10)
        for i in range (0, 10):
            tree [i], std [i] = str (i), str (i)
        validate (tree)

        # reload
        provider = self.provider (provider)
        tree = BPTree (provider)
        validate (tree)

        # Range
        self.assertEqual (list (tree [100:201]), [(key, str (key)) for key in range (100, 202)])
        self.assertEqual (list (tree [:100]), [(key, str (key)) for key in range (0, 101)])
        self.assertEqual (list (tree [101:102.5]), [(101, '101'), (102, '102')])

        # Deletion
        keys = list (range (count))
        half = len (keys) >> 1
        shuffle (keys)
        for i in keys [:half]:
            self.assertEqual (tree.pop (i), std.pop (i))
        validate (tree)

        # reload
        provider = self.provider (provider)
        tree = BPTree (provider)
        validate (tree)

        for i in keys [half:]:
            self.assertEqual (tree.pop (i), std.pop (i))
        self.assertEqual (len (tree), 0)
        validate (tree)

    def provider (self, source = None):
        if source is None:
            return SimpleProvider (order = 7)
        return source

#------------------------------------------------------------------------------#
# B+Tree with Sack Provider                                                    #
#------------------------------------------------------------------------------#
import io
from .sack import *
from .providers.sack import *

class BPTreeSackTest (BPTreeTest):
    def provider (self, source = None):
        if source is None:
            return SackProvider.Create (Sack.Create (io.BytesIO (), 32), 7)
        source.Flush ()
        return SackProvider (source.sack, source.desc)

# vim: nu ft=python columns=120 :
