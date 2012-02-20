# -*- coding: utf-8 -*-
"""Sack Bytes Provider

Sack based provider without pickle
"""
import sys
import struct
import array
import io
from bisect import bisect

# local
from . import Provider
from ..bptree import BPTreeNode, BPTreeLeaf

__all__ = ('SackBytesProvider',)
#------------------------------------------------------------------------------#
# B+Tree Sack Provider                                                         #
#------------------------------------------------------------------------------#
array_type = 'l'
class SackBytesProvider (Provider):
    def __init__ (self, sack, desc):
        """Load existing sack provider

        sack: sack used as backing store
        uid:  uid of header in the sack
        """
        self.sack = sack
        # Header:
        #   'I' order
        #   'I' depth
        #   'Q' size
        #   'Q' root descriptor
        ###
        self.header = struct.Struct ('!IIQQ')
        # Leaf Header:
        #   'Q' prev
        #   'Q' next
        ###
        self.leaf_header = struct.Struct ('!QQ')
        # Node Header:
        #   'H' count
        ###
        self.node_header = struct.Struct ('!H')

        self.d2n = {}
        self.dirty = set ()
        self.desc_next = -1

        self.desc = desc
        if desc is not None:
            header = sack.Get (desc)
            self.order, self.depth, self.size, root_desc = self.header.unpack_from (header)
            self.root = self.node_load (root_desc)

    @classmethod
    def Create (cls, sack, order):
        """Create new sack provider

        sack: sack used as backing store
        uid:  uid of header in the sack
        """
        provider = cls (sack, None)
        provider.order, provider.depth, provider.size = order, 1, 0
        provider.root = provider.NodeCreate ([], [], True)
        provider.desc = sack.Reserve (provider.header.size)
        provider.Flush ()

        return provider

    # TODO:
    #   1. Atomic save:
    #       Save all dirty nodes in new location, and if all dirty nodes has been saved
    #       successfully update header and free old locations, otherwise free successfully
    #       saved nodes and propagate exception.
    def Flush (self):
        """Flush cached values"""

        # relocated nodes
        d2n_reloc = {}

        #--------------------------------------------------------------------------#
        # Flush Leafs                                                              #
        #--------------------------------------------------------------------------#
        queue = []
        dirty_nodes = set ()
        for node in self.dirty:
            if node.is_leaf:
                # Leaf:
                #   0      1      9      17     ?
                #   +------+------+------+------+----------+
                #   | \x01 | prev | next | keys | children |
                #   +------+------+------+------+----------+
                ###
                data = io.BytesIO ()
                data.write (b'\x01')                  # set node flag
                data.seek (self.leaf_header.size + 1) # skip header
                SaveBytesList (node.keys, data)
                SaveBytesList (node.children, data)

                # enqueue
                queue.append ((node, data))

                # allocate space
                desc = self.sack.Reserve (data.tell (), None if node.desc < 0 else node.desc)

                # check if node has been relocated
                if node.desc != desc:
                    # queue parent for update
                    if node is not self.root:
                        parent, key = self.root, node.keys [0]
                        while True:
                            parent_desc = parent.children [bisect (parent.keys, key)]
                            if parent_desc == node.desc:
                                break
                            parent = self.d2n [parent_desc]
                        if parent not in self.dirty:
                            dirty_nodes.add (parent)

                    # update descriptor maps
                    self.d2n.pop (node.desc)
                    d2n_reloc [node.desc], node.desc = node, desc
                    self.d2n [desc] = node
            else:
                dirty_nodes.add (node)

        # all leafs has been allocated now
        for leaf, data in queue:
            # update prev
            prev = d2n_reloc.get (leaf.prev)
            if prev is not None:
                leaf.prev = prev.desc
            # update next
            next = d2n_reloc.get (leaf.next)
            if next is not None:
                leaf.next = next.desc

            # write header
            data.seek (1)
            data.write (self.leaf_header.pack (leaf.prev, leaf.next))

            # put leaf in sack
            desc = self.sack.Push (data.getvalue (), leaf.desc)
            assert leaf.desc == desc
        del queue

        #--------------------------------------------------------------------------#
        # Flush Nodes                                                              #
        #--------------------------------------------------------------------------#
        def node_flush (node):
            # flush children
            for index in range (len (node.children)):
                child_desc = node.children [index]
                child = d2n_reloc.get (child_desc)
                if child is not None:
                    # child has already been flushed
                    node.children [index] = child.desc
                else:
                    child = self.d2n.get (child_desc)
                    if child in dirty_nodes:
                        # flush child and update index
                        node.children [index] = node_flush (child)

            # Node:
            #   0      1       5      ?         + (8 * count)
            #   +------+-------+------+----------+
            #   | \x00 | count | keys | children |
            #   +------+-------+------+----------+
            ###
            data = io.BytesIO ()
            data.write (b'\x00') # unset leaf flag
            data.write (self.node_header.pack (len (node.children)))
            SaveBytesList (node.keys, data)
            node.children.tofile (data)

            # put node in sack
            desc = self.sack.Push (data.getvalue (), None if node.desc < 0 else node.desc)

            # check if node has been relocated
            if node.desc != desc:
                # queue parent for update
                if node is not self.root:
                    parent, key = self.root, node.keys [0]
                    while True:
                        parent_desc = parent.children [bisect (parent.keys, key)]
                        if parent_desc == node.desc:
                            break
                        parent = self.d2n [parent_desc]
                    if parent not in self.dirty:
                        dirty_nodes.add (parent)

                # update descriptor maps
                self.d2n.pop (node.desc)
                d2n_reloc [node.desc], node.desc = node, desc
                self.d2n [desc] = node

            # remove node from dirty set
            dirty_nodes.discard (node)

            return desc

        while dirty_nodes:
            node_flush (dirty_nodes.pop ())

        # clear dirty set
        self.dirty.clear ()

        #--------------------------------------------------------------------------#
        # Flush Header                                                             #
        #--------------------------------------------------------------------------#
        header = self.header.pack (self.order, self.depth, self.size, self.root.desc)
        self.sack.Push (header, self.desc)

        #--------------------------------------------------------------------------#
        # Flush Sack                                                               #
        #--------------------------------------------------------------------------#
        self.sack.Flush ()

    #--------------------------------------------------------------------------#
    # Provider Interface                                                       #
    #--------------------------------------------------------------------------#
    def NodeToDesc (self, node):
        return node.desc

    def DescToNode (self, desc):
        if desc:
            return self.d2n.get (desc) or self.node_load (desc)

    def Dirty (self, node):
        self.dirty.add (node)

    def Release (self, node):
        self.d2n.pop (node.desc)
        if node.desc >= 0:
            self.sack.Pop (node.desc)

    def NodeCreate (self, keys, children, is_leaf):
        desc, self.desc_next = self.desc_next, self.desc_next - 1
        if is_leaf:
            node = BPTreeSackLeaf (keys, children, desc)
        else:
            if isinstance (children, list):
                children = array.array (array_type, children)
            node = BPTreeSackNode (keys, children, desc)

        self.d2n [desc] = node
        self.dirty.add (node)
        return node

    def Size (self, value = None):
        self.size = self.size if value is None else value
        return self.size

    def Depth (self, value = None):
        self.depth = self.depth if value is None else value
        return self.depth

    def Root (self, value = None):
        self.root = self.root if value is None else value
        return self.root

    def Order (self):
        return self.order

    #--------------------------------------------------------------------------#
    # Private                                                                  #
    #--------------------------------------------------------------------------#
    def node_load (self, desc):
        data = io.BytesIO (self.sack.Get (desc))
        if data.read (1) == b'\x01':
            # Leaf:
            #   0      1      9      17     ?
            #   +------+------+------+------+----------+
            #   | \x01 | prev | next | keys | children |
            #   +------+------+------+------+----------+
            ###
            prev, next = self.leaf_header.unpack (data.read (self.leaf_header.size))
            keys, children = LoadBytesList (data), LoadBytesList (data)
            node = BPTreeSackLeaf (keys, children, desc)
            node.prev, node.next = prev, next
        else:
            # Node:
            #   0      1       5      ?         + (8 * count)
            #   +------+-------+------+----------+
            #   | \x00 | count | keys | children |
            #   +------+-------+------+----------+
            ###
            count = self.node_header.unpack (data.read (self.node_header.size)) [0]
            keys = list (LoadBytesList (data))
            children = array.array (array_type)
            children.fromfile (data, count)
            node = BPTreeSackNode (keys, children, desc)

        self.d2n [desc] = node
        return node

#------------------------------------------------------------------------------#
# Sack Provider Node                                                           #
#------------------------------------------------------------------------------#
class BPTreeSackNode (BPTreeNode):
    r"""B+Tree Node

    Dump Structure:
        0      1       5      ?         + (8 * count)
        +------+-------+------+----------+
        | \x00 | count | keys | children |
        +------+-------+------+----------+
    """
    __slots__ = ('keys', 'children', 'is_leaf', 'desc')

    def __init__ (self, keys, children, desc):
        BPTreeNode.__init__ (self, keys, children)
        self.desc = desc

class BPTreeSackLeaf (BPTreeLeaf):
    r"""B+Tree Leaf

    Dump Structure:
        0      1      9      17     ?          ?
        +------+------+------+------+----------+
        | \x01 | prev | next | keys | children |
        +------+------+------+------+----------+
    """
    __slots__ = ('keys', 'children', 'is_leaf', 'prev', 'next', 'desc')

    def __init__ (self, keys, children, desc):
        BPTreeLeaf.__init__ (self, keys, children)
        self.desc, self.prev, self.next = desc, 0, 0

#------------------------------------------------------------------------------#
# Save & Load Bytes List                                                       #
#------------------------------------------------------------------------------#
list_struct = struct.Struct ('!I')
def LoadBytesList (stream):
    # item count
    count = list_struct.unpack (stream.read (list_struct.size)) [0]
    # item sizes
    sizes = array.array ('I')
    sizes.fromfile (stream, count)
    # items
    return [stream.read (size) for size in sizes]

def SaveBytesList (list, stream):
    # item count
    stream.write (list_struct.pack (len (list)))
    # item sizes
    sizes = array.array ('I', (len (item) for item in list))
    sizes.tofile (stream)
    # items
    for item in list:
        stream.write (item)

# vim: nu ft=python columns=120 :
