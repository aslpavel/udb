# -*- coding: utf-8 -*-
import array
import struct

from ...utils import BytesList
from ...bptree import *

#------------------------------------------------------------------------------#
# Node                                                                         #
#------------------------------------------------------------------------------#
class Node (BPTreeNode):
    r"""B+Tree Bytes Node

    Dump Structure:
        0       4      ?         + (8 * count)
        +-------+------+----------+
        | count | keys | children |
        +-------+------+----------+
    """
    __slots__ = ('keys', 'children', 'is_leaf', 'desc')
    header = struct.Struct ('!H')
    array_type = 'l'

    def __init__ (self, keys, children, desc):
        BPTreeNode.__init__ (self,
            keys if isinstance (keys, BytesList) else BytesList (keys),
            children if isinstance (children, array.array) else array.array (self.array_type, children))
        self.desc = desc

    #--------------------------------------------------------------------------#
    # Chop                                                                     #
    #--------------------------------------------------------------------------#
    def Chop (self, index):
        keys, self.keys = BytesList (self.keys [index:]), BytesList (self.keys [:index])
        children, self.children = self.children [index:], self.children [:index]
        return keys, children

    #--------------------------------------------------------------------------#
    # Save | Load                                                              #
    #--------------------------------------------------------------------------#
    def SaveHeader (self, stream):
        stream.write (self.header.pack (len (self.children)))

    def Save (self, stream):
        self.keys.Save (stream)
        stream.write (self.children.tostring ())

    @classmethod
    def Load (cls, desc, stream):
        count = cls.header.unpack (stream.read (cls.header.size)) [0]
        keys = BytesList.Load (stream)
        children = array.array (cls.array_type)
        children.fromstring (stream.read (children.itemsize * count))

        return cls (keys, children, desc)

#------------------------------------------------------------------------------#
# Leaf                                                                         #
#------------------------------------------------------------------------------#
class Leaf (BPTreeLeaf):
    r"""B+Tree Bytes Leaf

    Dump Structure:
        0      8      16     ?          ?
        +------+------+------+----------+
        | prev | next | keys | children |
        +------+------+------+----------+
    """
    __slots__ = ('keys', 'children', 'is_leaf', 'prev', 'next', 'desc')
    header = struct.Struct ('!QQ')

    def __init__ (self, keys, children, desc):
        BPTreeLeaf.__init__ (self,
            keys if isinstance (keys, BytesList) else BytesList (keys),
            children if isinstance (children, BytesList) else BytesList (children))
        self.desc, self.prev, self.next = desc, 0, 0

    #--------------------------------------------------------------------------#
    # Chop                                                                     #
    #--------------------------------------------------------------------------#
    def Chop (self, index):
        keys, self.keys = BytesList (self.keys [index:]), BytesList (self.keys [:index])
        children, self.children = BytesList (self.children [index:]), BytesList (self.children [:index])
        return keys, children

    #--------------------------------------------------------------------------#
    # Save | Load                                                              #
    #--------------------------------------------------------------------------#
    def SaveHeader (self, stream):
        stream.write (self.header.pack (self.prev, self.next))

    def Save (self, stream):
        self.keys.Save (stream)
        self.children.Save (stream)

    @classmethod
    def Load (cls, desc, stream):
        prev, next = cls.header.unpack (stream.read (cls.header.size))
        keys = BytesList.Load (stream)
        children = BytesList.Load (stream)

        node = cls (keys, children, desc)
        node.prev, node.next = prev, next
        return node

# vim: nu ft=python columns=120 :
