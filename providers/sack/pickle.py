# -*- coding: utf-8 -*-
import sys
import array
import struct
if sys.version_info [0] < 3:
    import cPickle as pickle
else:
    import pickle

from ...bptree import *
#------------------------------------------------------------------------------#
# Node                                                                         #
#------------------------------------------------------------------------------#
pickle_version = 2
class Node (BPTreeNode):
    r"""B+Tree Pickle Node

    Dump Structure:
        0       4          count * 8        ?
        +-------+----------+----------------+
        | count | children | pickled (keys) |
        +-------+----------+----------------+
    """
    __slots__ = ('keys', 'children', 'is_leaf', 'desc')
    header = struct.Struct ('!H')
    array_type = 'l'

    def __init__ (self, keys, children, desc):
        BPTreeNode.__init__ (self, keys, children if isinstance (children, array.array)
            else array.array (self.array_type, children))
        self.desc = desc

    #--------------------------------------------------------------------------#
    # Save | Load                                                              #
    #--------------------------------------------------------------------------#
    def SaveHeader (self, stream):
        stream.write (self.header.pack (len (self.children)))

    def Save (self, stream):
        stream.write (self.children.tostring ())
        pickle.dump (self.keys, stream, pickle_version)

    @classmethod
    def Load (cls, desc, stream):
        count = cls.header.unpack (stream.read (cls.header.size)) [0]
        children = array.array (cls.array_type)
        children.fromstring (stream.read (count * children.itemsize))

        return cls (pickle.load (stream), children, desc)

#------------------------------------------------------------------------------#
# Leaf                                                                         #
#------------------------------------------------------------------------------#
class Leaf (BPTreeLeaf):
    r"""B+Tree Pickle Leaf

    Dump Structure:
        0      8      16                         ?
        +------+------+--------------------------+
        | prev | next | pickled (keys, children) |
        +------+------+--------------------------+
    """
    __slots__ = ('keys', 'children', 'is_leaf', 'prev', 'next', 'desc')
    header = struct.Struct ('!QQ')

    def __init__ (self, keys, children, desc):
        BPTreeLeaf.__init__ (self, keys, children)
        self.desc, self.prev, self.next = desc, 0, 0

    #--------------------------------------------------------------------------#
    # Save | Load                                                              #
    #--------------------------------------------------------------------------#
    def SaveHeader (self, stream):
        stream.write (self.header.pack (self.prev, self.next))

    def Save (self, stream):
        pickle.dump ((self.keys, self.children), stream, pickle_version)

    @classmethod
    def Load (cls, desc, stream):
        prev, next = cls.header.unpack (stream.read (cls.header.size))
        keys, children = pickle.load (stream)

        node = cls (keys, children, desc)
        node.prev, node.next = prev, next
        return node

# vim: nu ft=python columns=120 :
