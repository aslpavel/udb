# -*- coding: utf-8 -*-
import os
from .stream import *
from ..alloc import *

__all__ = ('FileSack',)
default_buffer_size = 1 << 16 # 64Kb

#------------------------------------------------------------------------------#
# File Sack                                                                    #
#------------------------------------------------------------------------------#
class FileSack (StreamSack):
    """File Sack

    Mode:
        'r' : Open existing sack for reading only
        'w' : Open existing sack for reading and writing
        'c' : Open existing sack for reading and writing, create if it doesn't exists
        'n' : Always create a new sack
    """
    def __init__ (self, file, mode = 'r', offset = 0, order = None):
        if mode in ('c', 'n') and order is None:
            raise ValueError ('order must be provided for \'c\' and \'n\' modes')

        if mode == 'r':
            StreamSack.__init__ (self, open (file, 'rb', buffering = default_buffer_size), offset)
        elif mode == 'w':
            StreamSack.__init__ (self, open (file, 'r+b', buffering = default_buffer_size), offset)
        elif mode == 'c':
            if not os.path.lexists (file):
                StreamSack.__init__ (self, open (file, 'w+b', buffering = default_buffer_size), offset, new = True)
                self.alloc, self.alloc_desc = BuddyAllocator (order), None
                self.Flush ()
            else:
                StreamSack.__init__ (self, open (file, 'r+b', buffering = default_buffer_size), offset)
        elif mode == 'n':
            StreamSack.__init__ (self, open (file, 'w+b', buffering = default_buffer_size), offset, new = True)
            self.alloc, self.alloc_desc = BuddyAllocator (order), None
            self.Flush ()
        else:
            raise ValueError ('s')

    @staticmethod
    def Create (cls, stream, order, offset = 0):
        raise NotImplementedError ('this method shluldn\'t be used')

    def __exit__ (self, et, eo, tb):
        self.stream.close ()
        return StreamSack.__exit__ (self, et, eo, tb)

# vim: nu ft=python columns=120 :
