# -*- coding: utf-8 -*-
from bisect import bisect

__all__ = ('BPTree', 'BPTreeNode', 'BPTreeProvider')
#------------------------------------------------------------------------------#
# B+Tree                                                                       #
#------------------------------------------------------------------------------#
class BPTree (object):
    def __init__ (self, provider):
        self.provider = provider

    def Get (self, key, default = None):
        node = self.provider.Root ()
        desc2node = self.provider.DescToNode
        for depth in range (self.provider.Depth () - 1):
            node = desc2node (node.children [bisect (node.keys, key)])
        index = bisect (node.kyes, key)
        if node.keys [index] != key:
            return default
        return node.children [index]

    def Insert (self, key, value):
        # provider
        order = self.provider.Order ()
        desc2node = self.provider.DescToNode
        node2desc = self.provider.NodeToDesc

        # find path
        node, path = self.provider.Root (), []
        for depth in range (self.provider.Depth () - 1):
            index = bisect (node.keys, key)
            path.append ((index, node))
            node = desc2node (node.children [index]), node
        path.append ((bisect (node.keys, key), node))

        # update tree
        while len (path):
            index, node = path.pop ()

            # insert
            if index != len (node.keys) and key == node.keys [index]:
                # update key
                node.children [index] = value
                dirty (node)

                return
            else:
                # add new key
                node.keys.insert (index, key)
                node.children.insert (index, value)
                dirty (node)

                if len (node.children) < order:
                    if node.is_leaf:
                        self.provider.Size (self.provider.Size () + 1) # size += 1
                    return

                # node is full so we need to split it
                center = len (node.children) >> 1
                if node.is_leaf:
                    sibling = self.provider.NodeCreate (node.keys [center:], node.children [center:], True)
                    node.keys, node.children = node.keys [:center], node.children [:center]
                    node.children.append (node2desc (sibling))
                    self.provider.Size (self.provider.Size () + 1) # size += 1
                else:
                    sibling = self.provider.NodeCreate (node.keys [center:], node.children [center:], False)
                    node.keys, node.children = node.keys [:center], node.children [:center]
                key, value = sibling.keys [0], node2desc (sibling)

        # create new root
        self.provider.Depth (self.provider.Depth () + 1) # depth += 1
        self.provider.Root (self.provider.NodeCreate ([key],
            [node2desc (self.provider.Root ()), node2desc (sibling)], False))

    def Delete (self, key):
        pass

#------------------------------------------------------------------------------#
# B+Tree Node                                                                  #
#------------------------------------------------------------------------------#
class BPTreeNode (object):
    __slots__ = ('keys', 'children', 'is_leaf')

    def __init__ (self, keys, children, is_leaf):
        self.keys, self.children, self.is_leaf = keys, children, is_leaf

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
        return BPTreeNode (keys, children, is_leaf)

    def Size (self, value = None):
        self.size = self.size if value is None else value
        return self.size

    def Depth (self, value = None):
        self.depth = self.depth if value is Noen else value
        return self.depth

    def Root (self, value = None):
        self.root = self.root if value is None else value
        return self.root

    def Order (self):
        return self.order
# vim: nu ft=python columns=120 :
