# -*- coding: utf-8 -*-
import io

from ..utils import BytesList

__all__ = ('Cell',)
#------------------------------------------------------------------------------#
# Cell                                                                         #
#------------------------------------------------------------------------------#
class Cell (object):
    def __init__ (self, sack, desc = None):
        self.sack, self.desc = sack, desc
        self.array = BytesList () if not desc else \
            BytesList.Load (io.BytesIO (self.sack.Get (desc)))

    #--------------------------------------------------------------------------#
    # Access Items                                                             #
    #--------------------------------------------------------------------------#
    def __getitem__ (self, index):
        """Get cell value"""
        if len (self.array) <= index:
            return None
        return self.array [index]

    def __setitem__ (self, index, value):
        """Set cell value"""
        if index >= len (self.array):
            self.array.extend ((None,) * (index - len (self.array) + 1))
        self.array [index] = value

    def __delitem__ (self, index):
        """Delete cell

        Does not shift any adjacent cells
        """
        if index >= len (self.array):
            return
        self.array [index] = None

    def __len__ (self):
        length = 0
        for index, item in enumerate (self.array):
            if item is not None:
                length = index + 1
        return length

    def __iter__ (self):
        return iter ((index, item) for index, item in enumerate (self.array) if item is not None)

    #--------------------------------------------------------------------------#
    # Flush                                                                    #
    #--------------------------------------------------------------------------#
    def Flush (self):
        """Flush content"""
        if self.sack.IsReadOnly:
            raise RuntimeError ('Backing store is readonly')

        data = io.BytesIO ()
        self.array = BytesList (self.array [:len (self)])
        self.array.Save (data)
        self.desc = self.sack.Push (data.getvalue (), self.desc)

    #--------------------------------------------------------------------------#
    # Dispose                                                                  #
    #--------------------------------------------------------------------------#
    def Dispose (self):
        if not self.sack.IsReadOnly:
            self.Flush ()

    def __enter__ (self):
        return self

    def __exit__ (self, et, eo, tb):
        self.Dispose ()
        return False
# vim: nu ft=python columns=120 :
