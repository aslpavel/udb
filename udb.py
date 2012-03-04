# -*- coding: utf-8 -*-
import contextlib

from .bptree import *
from .sack.file import *
from .providers.bytes import *

__all__ = ('uDB',)

#------------------------------------------------------------------------------#
# Default Values                                                               #
#------------------------------------------------------------------------------#
default_sack_order    = 32      # 4GB
default_bptree_order  = 32
default_provider_type = BytesProvider
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

        BPTree.__init__ (self, provider_type (self.sack, order = order, cell = cell))

    def Flush (self):
        self.provider.Flush ()

    @contextlib.contextmanager
    def Transaction (self):
        """Compatibility with old uDB

        TODO: remove it
        """
        yield
        self.provider.Flush ()

    def Close (self, flush = True):
        self.provider.Close (self.mode != 'r' and flush)

    def __enter__ (self):
        return self

    def __exit__ (self, et, eo, tb):
        self.Close (et is None)
        return False

# vim: nu ft=python columns=120 :
