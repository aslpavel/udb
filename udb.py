# -*- coding: utf-8 -*-
from .bptree import *
from .sack.file import *
from .providers import *
from .providers.sack import *

__all__ = ('uDB', 'xDB')
#------------------------------------------------------------------------------#
# Default Values                                                               #
#------------------------------------------------------------------------------#
default_sack_order    = 32
default_bptree_order  = 256
default_provider_type = 'SS'
default_cell          = 0

#------------------------------------------------------------------------------#
# Table                                                                        #
#------------------------------------------------------------------------------#
class Table (BPTree):
    def __init__ (self, sack, cell, order = None, provider_type = None, flags = None):
        # init defaults
        provider_type  = default_provider_type if provider_type is None  else provider_type
        order          = default_bptree_order  if order is None          else order

        # base ctor
        BPTree.__init__ (self, SackProvider (sack, order, provider_type, cell, flags))

    #--------------------------------------------------------------------------#
    # Flush                                                                    #
    #--------------------------------------------------------------------------#
    def Flush (self):
        self.provider.Flush ()

    #--------------------------------------------------------------------------#
    # Drop                                                                     #
    #--------------------------------------------------------------------------#
    def Drop (self):
        self.provider.Flush ()
        for node in list (self.provider):
            self.provider.sack.Pop (node.desc)
        del self.provider.sack.Cell [self.provider.cell]
        self.provider.sack.Flush ()
        self.provider = Provider () # set dummy provider

    #--------------------------------------------------------------------------#
    # Dispose                                                                  #
    #--------------------------------------------------------------------------#
    def Dispose (self):
        self.provider.Dispose ()

    def __enter__ (self):
        return self

    def __exit__ (self, et, eo, tb):
        if error [0] is None:
            self.provider.Dispose ()
        return False

#------------------------------------------------------------------------------#
# Micro Database                                                               #
#------------------------------------------------------------------------------#
class uDB (Table):
    def __init__ (self, file, mode = 'r', cell = None, order = None, capacity_order = None,
        provider_type = None, flags = None):
        # init defaults
        capacity_order = default_sack_order if capacity_order is None else capacity_order
        cell = default_cell if cell is None else cell

        # sack
        self.sack = FileSack (file, mode, capacity_order)

        # base ctor
        Table.__init__ (self, self.sack, cell, order, provider_type, flags)

    #--------------------------------------------------------------------------#
    # Dispose                                                                  #
    #--------------------------------------------------------------------------#
    def Dispose (self):
        Table.Dispose (self)
        self.sack.Dispose ()

    def __exit__ (self, *error):
        Table.__exit__ (self, et, eo, tb)
        self.sack.Dispose ()
        return False

#------------------------------------------------------------------------------#
# Multitable Database                                                          #
#------------------------------------------------------------------------------#
class xDB (object):
    def __init__ (self, file, mode = 'r', capacity_order = None):
        self.tables = {}
        self.sack = FileSack (file, mode, default_sack_order if capacity_order is None else capacity_order)

    #--------------------------------------------------------------------------#
    # Properties                                                               #
    #--------------------------------------------------------------------------#
    @property
    def Sack (self):
        return self.sack

    #--------------------------------------------------------------------------#
    # Access                                                                   #
    #--------------------------------------------------------------------------#
    def Table (self, cell, order = None, provider_type = None, flags = None):
        table = self.tables.get (cell)
        if table is None:
            table = Table (self.sack, cell, order, provider_type, flags)
            self.tables [cell] = table
        return table

    #--------------------------------------------------------------------------#
    # Dispose                                                                  #
    #--------------------------------------------------------------------------#
    def Dispose (self):
        for table in self.tables.values ():
            table.Dispose ()
        self.sack.Dispose ()

    def __enter__ (self):
        return self

    def __exit__ (self, et, eo, tb):
        self.Dispose ()
        return False
# vim: nu ft=python columns=120 :
