# -*- coding: utf-8 -*-

__all__ = ('BPTreeProvider',)
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

# vim: nu ft=python columns=120 :
