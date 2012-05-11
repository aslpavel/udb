# -*- coding: utf-8 -*-
import io

from .alloc import *
from .cell import *

from ..utils import *
from ..lock import *

__all__ = ('Sack',)
#------------------------------------------------------------------------------#
# Sack                                                                         #
#------------------------------------------------------------------------------#
class Sack (object):
    def __init__ (self, cell_desc, alloc_desc, order = None, readonly = None):
        self.readonly = False if readonly is None else readonly
        self.lock = DummyLock ()

        # allocator
        self.alloc_desc = alloc_desc
        if alloc_desc:
            self.alloc = BuddyAllocator.Restore (io.BytesIO (self.Get (self.alloc_desc)))
        else:
            if order is None:
                raise ValueError ('Order is required when creating new sack')
            self.alloc = BuddyAllocator (order)

        # cell
        self.cell = Cell (self, cell_desc)

    #--------------------------------------------------------------------------#
    # Access Data                                                              #
    #--------------------------------------------------------------------------#
    def Push (self, data, desc = None):
        raise NotImplementedError ('Abstract method')

    def Reserve (self, data, desc = None):
        raise NotImplementedError ('Abstract method')

    def Get (self, desc):
        raise NotImplementedError ('Abstract method')

    def Pop (self, desc):
        raise NotImplementedError ('Abstract method')

    #--------------------------------------------------------------------------#
    # Properties                                                               #
    #--------------------------------------------------------------------------#
    @property
    def Cell (self):
        return self.cell

    @property
    def IsReadOnly (self):
        return self.readonly

    #--------------------------------------------------------------------------#
    # Flush                                                                    #
    #--------------------------------------------------------------------------#
    def Flush (self):
        with self.WriteLock:
            if self.readonly:
                raise RuntimeError ('Sack is readonly')

            # flush cells
            self.Cell.Flush ()

            # flush allocator
            while True:
                state = io.BytesIO ()
                self.alloc.Save (state)
                desc = self.Push (state.getvalue (), self.alloc_desc)
                if desc == self.alloc_desc:
                    self.alloc_desc = desc
                    break
                self.alloc_desc = desc

    #--------------------------------------------------------------------------#
    # Locking                                                                  #
    #--------------------------------------------------------------------------#
    @property
    def ReadLock (self):
        return self.lock

    @property
    def WriteLock (self):
        return self.lock

    #--------------------------------------------------------------------------#
    # Dispose                                                                  #
    #--------------------------------------------------------------------------#
    def Dispose (self):
        if not self.readonly:
            self.Flush ()

    def __enter__ (self):
        return self

    def __exit__ (self, et, eo, tb):
        self.Dispose ()
        return False

# vim: nu ft=python columns=120 :
