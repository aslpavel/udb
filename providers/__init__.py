# -*- coding: utf-8 -*-

__all__ = ('Provider',)
#------------------------------------------------------------------------------#
# B+Tree Provider                                                              #
#------------------------------------------------------------------------------#
class Provider (object):

    #--------------------------------------------------------------------------#
    # Resolve References                                                       #
    #--------------------------------------------------------------------------#
    def NodeToDesc (self, node):
        raise NotImplementedError ()

    def DescToNode (self, desc):
        raise NotImplementedError ()

    #--------------------------------------------------------------------------#
    # Create | Delete                                                          #
    #--------------------------------------------------------------------------#
    def Dirty (self, node):
        raise NotImplementedError ()

    def Release (self, node):
        raise NotImplementedError ()

    def NodeCreate (self, keys, children, is_leaf):
        raise NotImplementedError ()

    def Flush (self):
        pass

    #--------------------------------------------------------------------------#
    # Properties                                                               #
    #--------------------------------------------------------------------------#
    def Size (self, value = None):
        raise NotImplementedError ()

    def Depth (self, value = None):
        raise NotImplementedError ()

    def Root (self, value = None):
        raise NotImplementedError ()

    def Order (self):
        raise NotImplementedError ()

    #--------------------------------------------------------------------------#
    # Dispose                                                                  #
    #--------------------------------------------------------------------------#
    def Dispose (self):
        self.Flush ()

    def __enter__ (self):
        return self

    def __exit__ (self, et, eo, tb):
        self.Dispose ()
        return False

# vim: nu ft=python columns=120 :
