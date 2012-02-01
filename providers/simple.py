# -*- coding: utf-8 -*-
from . import Provider
from ..bptree import BPTreeNode, BPTreeLeaf

__all__ = ('SimpleProvider',)
#------------------------------------------------------------------------------#
# B+Tree Simple Provider                                                       #
#------------------------------------------------------------------------------#
class SimpleProvider (Provider):
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
