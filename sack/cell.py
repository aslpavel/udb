# -*- coding: utf-8 -*-
import io
from ..utils import *

__all__ = ('Cell',)
#------------------------------------------------------------------------------#
# Cell                                                                         #
#------------------------------------------------------------------------------#
class Cell (object):
    def __init__ (self, sack, desc = None):
        self.sack, self.desc = sack, desc
        self.array = BytesList () if not desc else \
            BytesList.Load (io.BytesIO (self.sack.Get (desc)))

    def __getitem__ (self, index):
        """Get cell value"""
        if len (self.array) <= index:
            return b''
        return self.array [index]

    def __setitem__ (self, index, value):
        """Set cell value"""
        if index >= len (self.array):
            self.array.extend ((b'',) * (index - len (self.array) + 1))
        self.array [index] = value

    def __delitem__ (self, index):
        """Delete cell

        Does not shift any adjacent cells
        """
        if index >= len (self.array):
            return
        self.array [index] = b''

    def __len__ (self):
        length = 0
        for index, item in enumerate (self.array):
            if len (item) > 0:
                length = index + 1
        return length

    def Flush (self):
        """Flush content"""
        data = io.BytesIO ()
        self.array = BytesList (self.array [:len (self)])
        self.array.Save (data)
        self.desc = self.sack.Push (data.getvalue (), self.desc)

    def Close (self, flush = True):
        if flush:
            self.Flush ()

    def __enter__ (self):
        return self

    def __exit__ (self, et, eo, tb):
        self.Close ()
        return False
# vim: nu ft=python columns=120 :
