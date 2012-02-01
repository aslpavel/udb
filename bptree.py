# -*- coding: utf-8 -*-
import sys
from bisect import bisect, bisect_left
from collections import MutableMapping
from operator import itemgetter
if sys.version_info [0] < 3:
    from itertools import imap as map

__all__ = ('BPTree', 'BPTreeNode',
   'BPTreeProvider', 'BPTreeSimpleProvider')

null = object ()
#------------------------------------------------------------------------------#
# B+Tree                                                                       #
#------------------------------------------------------------------------------#
class BPTree (MutableMapping):
    def __init__ (self, provider):
        self.provider = provider

    def Get (self, key, default = null):
        # provider
        desc2node = self.provider.DescToNode

        # find leaf
        node = self.provider.Root ()
        for depth in range (self.provider.Depth () - 1):
            node = desc2node (node.children [bisect (node.keys, key)])

        # check key
        index = bisect_left (node.keys, key)
        if index >= len (node.keys) or key != node.keys [index]:
            if default is null:
                raise KeyError (key)
            return default

        return node.children [index]

    def GetCursor (self, key):
        desc2node = self.provider.DescToNode
        node = self.provider.Root ()
        for depth in range (self.provider.Depth () - 1):
            node = desc2node (node.children [bisect (node.keys, key)])

        return BPTreeCursor (self.provider, node, bisect_left (node.keys, key) - 1)

    def GetRange (self, low = None, high = None):
        # validate range
        if low is not None and high is not None and low >= high:
            return

        # find first leaf
        desc2node = self.provider.DescToNode
        node = self.provider.Root ()
        if low is not None:
            for depth in range (self.provider.Depth () - 1):
                node = desc2node (node.children [bisect (node.keys, low)])
            index = bisect_left (node.keys, low)
            if index >= len (node.keys):
                if node.next is None:
                    return
                node, index = desc2node (node.next), 0
        else:
            for depth in range (self.provider.Depth () - 1):
                node = desc2node (node.children [0])
            index = 0

        # iterate over whole leafs
        while not high or node.keys [-1] < high:
            for index in range (index, len (node.keys)):
                yield node.keys [index], node.children [index]
            node = desc2node (node.next)
            if node is None:
                return
            index = 0

        # iterate over last leaf
        for index in range (index, len (node.keys)):
            key, value = node.keys [index], node.children [index]
            if key > high:
                return
            yield key, value

    def Add (self, key, value):
        # provider
        order = self.provider.Order ()
        dirty = self.provider.Dirty
        desc2node = self.provider.DescToNode
        node2desc = self.provider.NodeToDesc

        # find path
        node, path = self.provider.Root (), []
        for depth in range (self.provider.Depth () - 1):
            index = bisect (node.keys, key)
            path.append ((index, index + 1, node))
            node = desc2node (node.children [index])

        # check if value is updated
        index = bisect_left (node.keys, key)
        if index < len (node.keys) and key == node.keys [index]:
            node.children [index] = value
            dirty (node)
            return
        path.append ((index, index, node))

        # size += 1
        self.provider.Size (self.provider.Size () + 1)

        # update tree
        sibling = None
        while path:
            key_index, child_index, node = path.pop ()

            # add new key
            node.keys.insert (key_index, key)
            node.children.insert (child_index, value)
            dirty (node)

            if len (node.keys) < order:
                return

            # node is full so we need to split it
            center = len (node.children) >> 1
            if node.is_leaf:
                # create right sibling
                sibling = self.provider.NodeCreate (node.keys [center:], node.children [center:], True)
                node.keys, node.children = node.keys [:center], node.children [:center]

                # keep leafs linked
                sibling_desc, node_next_desc = node2desc (sibling), node.next
                node.next, sibling.prev = sibling_desc, node2desc (node)
                if node_next_desc:
                    node_next = desc2node (node_next_desc)
                    node_next.prev, sibling.next = sibling_desc, node_next_desc
                    dirty (node_next)

                # update key
                key, value = sibling.keys [0], node.next

            else:
                # create right sibling
                sibling = self.provider.NodeCreate (node.keys [center:], node.children [center:], False)
                node.keys, node.children = node.keys [:center], node.children [:center]

                # update key
                key, value = node.keys.pop (), node2desc (sibling)

            dirty (sibling)

        # create new root
        self.provider.Depth (self.provider.Depth () + 1) # depth += 1
        self.provider.Root (self.provider.NodeCreate ([key],
            [node2desc (self.provider.Root ()), node2desc (sibling)], False))

    def Pop (self, key, default = null):
        # provider
        half_order = self.provider.Order () >> 1
        dirty = self.provider.Dirty
        desc2node = self.provider.DescToNode

        # find path
        node, path = self.provider.Root (), []
        for depth in range (self.provider.Depth () - 1):
            index = bisect (node.keys, key)
            parent, node = node, desc2node (node.children [index])
            path.append ((node, index, parent))

        # check if key exists
        index = bisect_left (node.keys, key)
        if index >= len (node.keys) or key != node.keys [index]:
            if default is null:
                raise KeyError (key)
            return default
        value = node.children [index]
        key_index, child_index = index, index

        # size -= 1
        self.provider.Size (self.provider.Size () - 1)

        # update tree
        while path:
            node, node_index, parent = path.pop ()

            # remove scheduled (key | child)
            del node.keys [key_index]
            del node.children [child_index]

            if len (node.keys) >= half_order:
                dirty (node)
                return value

            #------------------------------------------------------------------#
            # Redistribute                                                     #
            #------------------------------------------------------------------#
            left, right = None, None
            if node_index > 0:
                # has left sibling
                left = desc2node (parent.children [node_index - 1])
                if len (left.keys) > half_order: # borrow from left sibling
                    # copy correct key to node
                    node.keys.insert (0, left.keys [-1] if node.is_leaf
                        else parent.keys [node_index - 1])
                    # move left key to parent
                    parent.keys [node_index - 1] = left.keys.pop ()
                    # move left child to node
                    node.children.insert (0, left.children.pop ())

                    dirty (node), dirty (left), dirty (parent)
                    return value

            if node_index < len (parent.keys):
                # has right sibling
                right = desc2node (parent.children [node_index + 1])
                if len (right.keys) > half_order: # borrow from right sibling
                    if node.is_leaf:
                        # move right key to node
                        node.keys.append (right.keys.pop (0))
                        # copy next right key to parent
                        parent.keys [node_index] = right.keys [0]
                    else:
                        # copy correct key to node
                        node.keys.append (parent.keys [node_index])
                        # move right key to parent
                        parent.keys [node_index] = right.keys.pop (0)
                    # move right child to node
                    node.children.append (right.children.pop (0))

                    dirty (node), dirty (right), dirty (parent)
                    return value

            #------------------------------------------------------------------#
            # Merge                                                            #
            #------------------------------------------------------------------#
            src, dst, child_index = ((node, left, node_index) if left
                else (right, node, node_index + 1))

            if node.is_leaf:
                # keep leafs linked
                dst.next = src.next
                if src.next is not None:
                    src_next = desc2node (src.next)
                    src_next.prev = src.prev
                    dirty (src_next)
            else:
                # copy parents key
                dst.keys.append (parent.keys [child_index - 1])

            # copy node's (keys | children)
            dst.keys.extend (src.keys)
            dst.children.extend (src.children)

            # mark nodes
            self.provider.Release (src)
            dirty (dst)

            # update key index
            key_index = child_index - 1

        #----------------------------------------------------------------------#
        # Update Root                                                          #
        #----------------------------------------------------------------------#
        root = self.provider.Root ()
        del root.keys [key_index]
        del root.children [child_index]

        if not root.keys:
            depth = self.provider.Depth ()
            if depth > 1:
                # root is not leaf because depth > 1
                self.provider.Root (desc2node (*root.children))
                self.provider.Release (root)
                self.provider.Depth (depth - 1) # depth -= 1
        else:
            dirty (root)

        return value

    #--------------------------------------------------------------------------#
    # Mutable Map Interface                                                    #
    #--------------------------------------------------------------------------#
    def __len__ (self):
        return self.provider.Size ()

    def __getitem__ (self, key):
        if isinstance (key, slice):
            return self.GetRange (low = key.start, high = key.stop)
        return self.Get (key)

    def __setitem__ (self, key, value):
        return self.Add (key, value)

    def __delitem__ (self, key):
        return self.Pop (key)

    def __iter__ (self):
        return map (itemgetter (0), self.GetRange ())

    def __contains__ (self, key):
        return self.Get (key, default = None) is not None

    def get (self, key, default = None):
        return self.Get (key, default)

    def pop (self, key, default = None):
        return self.Pop (key, default)

    def items (self):
        return self.GetRange ()

    def values (self):
        return map (itemgetter (1), self.GetRange ())

#------------------------------------------------------------------------------#
# B+Tree Cursor                                                                #
#------------------------------------------------------------------------------#
class BPTreeCursor (object):
    __slots__ = ('leaf', 'index', 'completed', 'provider')

    def __init__ (self, provider, leaf, index):
        self.leaf = leaf
        self.index = index
        self.provider = provider
        self.completed = False

    def __iter__ (self):
        return self

    def __next__ (self):
        if self.completed:
            raise StopIteration ()

        leaf = self.leaf
        if len (leaf.keys) > self.index + 1:
            self.index += 1
            return leaf.keys [self.index], leaf.children [self.index]

        if leaf.next is None:
            self.completed = True
            raise StopIteration ()

        self.leaf = self.provider.DescToNode (leaf.next)
        self.index = 0
        return self.__next__ ()

    def next (self):
        return self.__next__ ()

    def __reversed__ (self):
        return BPTreeReversedCursor (self.provider, self.leaf, self.index + 1)

    def __invert__ (self):
        return self.__reversed__ ()

    @property
    def Value (self):
        return self.leaf.children [self.index]

    @Value.setter
    def Value (self, value):
        self.leaf.children [self.index] = value
        self.provider.Dirty (self.leaf)

    @property
    def Key (self):
        return self.leaf.keys [self.index]

class BPTreeReversedCursor (BPTreeCursor):
    __slots__ = ('leaf', 'index', 'completed', 'provider')

    def __next__ (self):
        if self.completed:
            raise StopIteration ()

        leaf = self.leaf
        if self.index > 0:
            self.index -= 1
            return leaf.keys [self.index], leaf.children [self.index]

        if leaf.prev is None:
            self.completed = True
            raise StopIteration ()

        self.leaf = self.provider.DescToNode (leaf.prev)
        self.index = len (self.leaf.keys)
        return self.__next__ ()

    def __reversed__ (self):
        return BPTreeCursor (self.provider, self.leaf, self.index - 1)

#------------------------------------------------------------------------------#
# B+Tree Node                                                                  #
#------------------------------------------------------------------------------#
class BPTreeNode (object):
    __slots__ = ('keys', 'children', 'is_leaf',)

    def __init__ (self, keys, children, is_leaf = False):
        self.keys, self.children, self.is_leaf = keys, children, is_leaf

class BPTreeLeaf (BPTreeNode):
    __slots__ = ('keys', 'children', 'is_leaf', 'next', 'prev')

    def __init__ (self, keys, children):
        BPTreeNode.__init__ (self, keys, children, is_leaf = True)
        self.next, self.prev = None, None

#------------------------------------------------------------------------------#
# B+Tree Provider                                                              #
#------------------------------------------------------------------------------#
class BPTreeProvider (object):
    # transformation
    def NodeToDesc (self, node):
        raise NotImplementedError ()

    def DescToNode (self, desc):
        raise NotImplementedError ()

    # creation | deletion
    def Dirty (self, node):
        raise NotImplementedError ()

    def Release (self, node):
        raise NotImplementedError ()

    def NodeCreate (self, keys, children, is_leaf):
        raise NotImplementedError ()

    # properties
    def Size (self, value = None):
        raise NotImplementedError ()

    def Depth (self, value = None):
        raise NotImplementedError ()

    def Root (self, value = None):
        raise NotImplementedError ()

    def Order (self):
        pass

#------------------------------------------------------------------------------#
# B+Tree Simple Provider                                                       #
#------------------------------------------------------------------------------#
class BPTreeSimpleProvider (BPTreeProvider):
    def __init__ (self, order):
        self.root = self.NodeCreate ([], [], True)
        self.size  = 0
        self.depth = 1
        self.order = order

    def NodeToDesc (self, node):
        return node

    def DescToNode (self, desc):
        return desc

    def Dirty (self, node):
        pass

    def Release (self, node):
        pass

    def NodeCreate (self, keys, children, is_leaf):
        return (BPTreeLeaf (keys, children) if is_leaf
            else BPTreeNode (keys, children))

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
# vim: nu ft=python columns=120 :
