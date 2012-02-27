# -*- coding: utf-8 -*-
import sys
import struct
import array
import io
from bisect import bisect
if sys.version_info [0] < 3:
    import cPickle as pickle
else:
    import pickle

# local
from . import Provider
from ..bptree import BPTreeNode, BPTreeLeaf

__all__ = ('PickleProvider',)
#------------------------------------------------------------------------------#
# B+Tree Pickle Sack Provider                                                  #
#------------------------------------------------------------------------------#
array_type = 'l'
class PickleProvider (Provider):
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
        #   'B' pickle version
        ###
        self.header = struct.Struct ('!IIQQB')
        # Leaf Header:
        #   'Q' prev
        #   'Q' next
        ###
        self.leaf_header = struct.Struct ('!QQ')
        # Node Header:
        #   'H' children count
        ###
        self.node_header = struct.Struct ('!H')

        self.d2n = {}
        self.dirty = set ()
        self.desc_next = -1

        self.desc = desc
        if desc is not None:
            header = sack.Get (desc)
            self.order, self.depth, self.size, root_desc, self.pickle_version = self.header.unpack_from (header)
            self.root = self.node_load (root_desc)

    @classmethod
    def Create (cls, sack, order):
        """Create new sack provider

        sack: sack used as backing store
        uid:  uid of header in the sack
        """
        provider = cls (sack, None)
        provider.order, provider.depth, provider.size = order, 1, 0
        provider.pickle_version = pickle.HIGHEST_PROTOCOL
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
                #   0      1      9      17
                #   +------+------+------+--------------------------+
                #   | \x01 | prev | next | pickled (keys, children) |
                #   +------+------+------+--------------------------+
                ###
                data = io.BytesIO ()
                data.write (b'\x01')                  # set node flag
                data.seek (self.leaf_header.size + 1) # skip header
                pickle.dump ((node.keys, node.children), data, self.pickle_version)

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
            #   0      1       3          count * 8
            #   +------+-------+----------+----------------+
            #   | \x00 | count | children | pickled (keys) |
            #   +------+-------+----------+----------------+
            ###
            data = io.BytesIO ()
            data.write (b'\x00') # unset leaf flag
            data.write (self.node_header.pack (len (node.children)))
            node.children.tofile (data)
            pickle.dump (node.keys, data, self.pickle_version)

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
        header = self.header.pack (self.order, self.depth, self.size, self.root.desc, self.pickle_version)
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
        data = self.sack.Get (desc)
        is_leaf, data = data [0] != 0, data [1:]
        if is_leaf:
            # Leaf:
            #   0      1      9      17
            #   +------+------+------+--------------------------+
            #   | \x01 | prev | next | pickled (keys, children) |
            #   +------+------+------+--------------------------+
            ###
            prev, next = self.leaf_header.unpack_from (data)
            keys, children = pickle.loads (data [self.leaf_header.size:])
            node = BPTreeSackLeaf (keys, children, desc)
            node.prev, node.next = prev, next
        else:
            # Node:
            #   0      1       3          count * 8
            #   +------+-------+----------+----------------+
            #   | \x00 | count | children | pickled (keys) |
            #   +------+-------+----------+----------------+
            ###
            count = self.node_header.unpack_from (data) [0]
            children, data = array.array (array_type), data [self.node_header.size:]
            count *= children.itemsize
            children.frombytes (data [:count])
            keys = pickle.loads (data [count:])
            node = BPTreeSackNode (keys, children, desc)

        self.d2n [desc] = node
        return node

#------------------------------------------------------------------------------#
# Sack Provider Node                                                           #
#------------------------------------------------------------------------------#
class BPTreeSackNode (BPTreeNode):
    __slots__ = ('keys', 'children', 'is_leaf', 'desc')

    def __init__ (self, keys, children, desc):
        BPTreeNode.__init__ (self, keys, children)
        self.desc = desc

class BPTreeSackLeaf (BPTreeLeaf):
    __slots__ = ('keys', 'children', 'is_leaf', 'prev', 'next', 'desc')

    def __init__ (self, keys, children, desc):
        BPTreeLeaf.__init__ (self, keys, children)
        self.desc, self.prev, self.next = desc, 0, 0

# vim: nu ft=python columns=120 :
