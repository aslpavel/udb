# -*- coding: utf-8 -*-
import io
from .alloc import *
from .cell import *
from ..utils import *

__all__ = ('Sack',)
#------------------------------------------------------------------------------#
# Sack                                                                         #
#------------------------------------------------------------------------------#
class Sack (object):
    def __init__ (self, cell_desc, alloc_desc, order = None):
        # allocator
        self.alloc_desc = alloc_desc
        if alloc_desc:
            self.alloc = BuddyAllocator.Restore (io.BytesIO (self.Get (self.alloc_desc)))
        else:
            if order is None:
                raise ValueError ('Order is required when creating new sack')
            self.alloc = BuddyAllocator (order)

        # cell
        self.Cell = Cell (self, cell_desc)

        # events
        self.OnFlush = Event ()

    def Get (self, desc):
        raise NotImplementedError ('Abstract method')

    def Push (self, data, desc = None):
        raise NotImplementedError ('Abstract method')

    def Flush (self):
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

        self.OnFlush (self)

    def Close (self, flush = True):
        if flush:
            self.Flush ()

    # context manager
    def __enter__ (self):
        return self

    def __exit__ (self, et, eo, tb):
        self.Close ()
        return False

# vim: nu ft=python columns=120 :
