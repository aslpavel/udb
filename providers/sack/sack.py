# -*- coding: utf-8 -*-
"""Sack Provider"""
import io
import struct
import zlib
from bisect import bisect

# local
from .. import Provider

__all__ = ('SackProvider',)
#------------------------------------------------------------------------------#
# B+Tree Sack Provider                                                         #
#------------------------------------------------------------------------------#
class SackProvider (Provider):
    #--------------------------------------------------------------------------#
    # Flags                                                                    #
    #--------------------------------------------------------------------------#
    FLAG_COMPRESSION = 1

    def __init__ (self, sack, order = None, type = None, cell = 0, flags = 0):
        """Sack Provider

        sack  : sack backing store
        order : maximum children count inside node
        cell  : cell with header
        type  : sack type
        """
        self.sack = sack
        self.order = order
        self.type = type

        # Header:
        #   '2s' type
        #   'B'  flags
        #   'I'  order
        #   'I'  depth
        #   'Q'  size
        #   'Q'  root descriptor
        ###
        self.header = struct.Struct ('!2sBIIQQ')

        self.d2n = {}
        self.dirty = set ()
        self.desc_next = -1

        self.cell = cell
        header = self.sack.Cell [cell]
        if header:
            type , self.flags, self.order, self.depth, self.size, root_desc = self.header.unpack_from (header)
            type = type.decode ('utf-8')
            if self.type and self.type != type:
                raise ValueError ('Type mismatch requested \'{}\', but found \'{}\''.format (self.type, type))
            self.type_resolve (type)
            self.root = self.node_load (root_desc)
        else:
            # cell is not set create new provider
            if order is None:
                raise ValueError ('Order is required to create new provider')
            if type is None:
                raise ValueError ('Type is required to create new provider')

            # init provider
            self.type_resolve (type)
            self.flags = flags
            self.order = order
            self.depth = 1
            self.size  = 0
            self.root = self.NodeCreate ([], [], True)

            self.Flush ()

    def Flush (self):
        """Flush cached values"""

        # relocated nodes
        d2n_reloc = {}

        #--------------------------------------------------------------------------#
        # Flush Leafs                                                              #
        #--------------------------------------------------------------------------#
        leaf_queue = {}
        def leaf_enqueue (leaf):
            data = io.BytesIO ()
            data.write (b'\x01') # set node flag
            leaf.Save (data)

            # enqueue leaf
            leaf_queue [leaf] = data

            # allocate space
            desc = self.sack.Reserve (data.tell (), None if leaf.desc < 0 else leaf.desc)

            # check if node has been relocated
            if leaf.desc != desc:
                # queue parent for update
                if leaf is not self.root:
                    parent, key = self.root, leaf.keys [0]
                    while True:
                        parent_desc = parent.children [bisect (parent.keys, key)]
                        if parent_desc == leaf.desc:
                            break
                        parent = self.DescToNode (parent_desc)
                    if parent not in self.dirty:
                        node_queue.add (parent)

                # queue next and previous for update
                for sibling_desc in (leaf.prev, leaf.next):
                    # descriptor is negative, node is dirty
                    if (sibling_desc > 0 and                # negative node is dirty for sure
                        sibling_desc not in d2n_reloc):     # relocated node is also dirty

                        sibling = self.d2n.get (sibling_desc)
                        if sibling:
                            # node has already been loaded
                            if (sibling not in self.dirty and
                                sibling not in leaf_queue):
                                    # queue it for update
                                    leaf_enqueue (sibling)
                        else:
                            # node hasn't been loaded
                            leaf_enqueue (self.node_load (sibling_desc))

                # update descriptor maps
                self.d2n.pop (leaf.desc)
                d2n_reloc [leaf.desc], leaf.desc = leaf, desc
                self.d2n [desc] = leaf

        # enqueue leafs and create dirty nodes queue
        node_queue = set ()
        for node in self.dirty:
            if node.is_leaf:
                leaf_enqueue (node)
            else:
                node_queue.add (node)

        # all leafs has been allocated now
        for leaf, data in leaf_queue.items ():
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
            leaf.SaveHeader (data)

            # put leaf in sack
            if self.flags & self.FLAG_COMPRESSION:
                desc = self.sack.Push (zlib.compress (data.getvalue ()), leaf.desc)
            else:
                desc = self.sack.Push (data.getvalue (), leaf.desc)
            assert leaf.desc == desc

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
                    if child in node_queue:
                        # flush child and update index
                        node.children [index] = node_flush (child)

            # save
            data = io.BytesIO ()
            data.write (b'\x00') # unset leaf flag
            node.Save (data)

            # put node in sack
            if self.flags & self.FLAG_COMPRESSION:
                desc = self.sack.Push (zlib.compress (data.getvalue ()), None if node.desc < 0 else node.desc)
            else:
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
                        node_queue.add (parent)

                # update descriptor maps
                self.d2n.pop (node.desc)
                d2n_reloc [node.desc], node.desc = node, desc
                self.d2n [desc] = node

            # remove node from dirty set
            node_queue.discard (node)

            return desc

        while node_queue:
            node_flush (node_queue.pop ())

        # clear dirty set
        self.dirty.clear ()

        #--------------------------------------------------------------------------#
        # Flush Header                                                             #
        #--------------------------------------------------------------------------#
        self.sack.Cell [self.cell] = self.header.pack (self.type.encode ('utf-8'), self.flags, self.order,
            self.depth, self.size, self.root.desc)

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
        self.dirty.discard (node)
        if node.desc >= 0:
            self.sack.Pop (node.desc)

    def NodeCreate (self, keys, children, is_leaf):
        desc, self.desc_next = self.desc_next, self.desc_next - 1
        node = (self.leaf_type (keys, children, desc) if is_leaf else
            self.node_type (keys, children, desc))

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
    # Dispose                                                                  #
    #--------------------------------------------------------------------------#
    def Dispose (self):
        if not self.sack.IsReadOnly:
            self.Flush ()

    #--------------------------------------------------------------------------#
    # Private                                                                  #
    #--------------------------------------------------------------------------#
    def node_load (self, desc):
        if self.flags & self.FLAG_COMPRESSION:
            data = io.BytesIO (zlib.decompress (self.sack.Get (desc)))
        else:
            data = io.BytesIO (self.sack.Get (desc))

        node = (self.leaf_type.Load (desc, data) if data.read (1) == b'\x01' else
            self.node_type.Load (desc, data))

        self.d2n [desc] = node
        return node

    def type_resolve (self, type):
        self.type = type
        if type == 'SS':
            from .bytes import Node, Leaf
            self.node_type, self.leaf_type = Node, Leaf
        elif type == 'PP':
            from .pickle import Node, Leaf
            self.node_type, self.leaf_type = Node, Leaf
        else:
            raise TypeError ('Unsupported type \'{}\''.format (type))
# vim: nu ft=python columns=120 :
