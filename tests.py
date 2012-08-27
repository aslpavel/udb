# -*- coding: utf-8 -*-
from __future__ import print_function
import io
import unittest

from .bptree import BPTree
from .sack.stream import StreamSack
from .providers.simple import SimpleProvider
from .providers.sack import SackProvider

from random import shuffle
#------------------------------------------------------------------------------#
# Sack                                                                         #
#------------------------------------------------------------------------------#
class TestSack (unittest.TestCase):
    def test_Create (self):
        StreamSack (io.BytesIO (), offset = 10, order = 32, new = True)

    def test_Load (self):
        stream = io.BytesIO ()
        StreamSack (stream, offset = 10, order = 32, new = True).Flush ()
        StreamSack (stream, offset = 10)

    def test_PushPop (self):
        stream = io.BytesIO ()
        with StreamSack (stream, 10, 32, True) as sack: # new sack
            d0 = sack.Push (b'some data')
            d1 = sack.Push (b'some large data' * 100)
            d2 = sack.Push (b'test')

        with StreamSack (stream, 10) as sack:
            self.assertEqual (sack.Get (d0), b'some data')
            self.assertEqual (sack.Pop (d1), b'some large data' * 100)

        with StreamSack (stream, 10) as sack:
            self.assertEqual (sack.Get (d2), b'test')
            d2_new = sack.Push (b'test' * 10, d2)
            self.assertNotEqual (d2_new, d2)
            self.assertEqual (sack.Push (b'abc', d1), d1)

    def test_Cell (self):
        stream = io.BytesIO ()
        with StreamSack (stream, 10, 32, True) as sack:
            self.assertEqual (len (sack.Cell), 0)
            sack.Cell [1] = b'test'
            self.assertEqual (len (sack.Cell), 2)

        with StreamSack (stream, 10) as sack:
            self.assertEqual (len (sack.Cell), 2)
            self.assertEqual (sack.Cell [1], b'test')

            # empty
            self.assertEqual (sack.Cell [0], None)
            self.assertEqual (sack.Cell [2], None)

            del sack.Cell [1]
            self.assertEqual (len (sack.Cell), 0)

        with StreamSack (stream, 10) as sack:
            self.assertEqual (len (sack.Cell.array), 0)

#------------------------------------------------------------------------------#
# B+Tree                                                                       #
#------------------------------------------------------------------------------#
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

        # Cursor
        cursor = tree.GetCursor (11)
        self.assertEqual (next (cursor), (11, '11'))
        next (cursor)
        self.assertEqual ((cursor.Key, cursor.Value), (12, '12'))
        cursor.Value = '12'
        cursor = ~cursor
        self.assertEqual (list (cursor), [(12, '12'), (11, '11'), (10, '10')])
        cursor = ~cursor
        next (cursor)
        self.assertEqual ((cursor.Key, cursor.Value), (10, '10'))

        cursor = tree.GetCursor (1023)
        self.assertEqual (list (cursor), [(1023, '1023')])

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
        self.assertEqual (list (tree [1022:]), [(1022, '1022'), (1023, '1023')])
        self.assertEqual (list (tree [100:10]), [])

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
class BPTreeSackTest (BPTreeTest):
    def provider (self, source = None):
        if source is None:
            return SackProvider (StreamSack (io.BytesIO (), order = 32, new = True, readonly = False), order = 7, type = 'PP')
        source.Flush ()
        return SackProvider (StreamSack (source.sack.stream, source.sack.offset))

# vim: nu ft=python columns=120 :
