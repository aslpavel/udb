# -*- coding: utf-8 -*-
import contextlib

from .bptree import *
from .sack.file import *
from .providers.sack import *

__all__ = ('uDB',)

#------------------------------------------------------------------------------#
# Default Values                                                               #
#------------------------------------------------------------------------------#
default_sack_order    = 32      # 4GB
default_bptree_order  = 32
default_provider_type = 'SS'
default_cell          = 0

#------------------------------------------------------------------------------#
# Micro Database Interface                                                     #
#------------------------------------------------------------------------------#
class uDB (BPTree):
    def __init__ (self, file, mode = 'r', order = None, cell = None, capacity_order = None, provider_type = None):
        # init defaults
        provider_type  = default_provider_type if provider_type is None else provider_type
        capacity_order = default_sack_order if capacity_order is None else capacity_order
        order          = default_bptree_order if order is None else order
        cell           = default_cell if cell is None else cell

        # open sack
        self.mode = mode
        self.sack = FileSack (file, mode = mode, order = capacity_order)

        BPTree.__init__ (self, SackProvider (self.sack, order = order, type = provider_type, cell = cell))

    #--------------------------------------------------------------------------#
    # Flush                                                                    #
    #--------------------------------------------------------------------------#
    def Flush (self):
        self.provider.Flush ()

    #--------------------------------------------------------------------------#
    # Dispose                                                                  #
    #--------------------------------------------------------------------------#
    def Dispose (self):
        self.provider.Dispose ()
        self.sack.Dispose ()

    def __enter__ (self):
        return self

    def __exit__ (self, *error):
        if error [0] is None:
            self.provider.Dispose ()
        self.sack.Dispose ()
        return False

    #--------------------------------------------------------------------------#
    # Deprecated                                                               #
    #--------------------------------------------------------------------------#
    @contextlib.contextmanager
    def Transaction (self):
        """Compatibility with old uDB

        TODO: remove it
        """
        yield
        self.provider.Flush ()

# vim: nu ft=python columns=120 :
