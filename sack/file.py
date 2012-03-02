# -*- coding: utf-8 -*-
import os
from .stream import *
from .alloc import *

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
        self.mode = mode
        if mode in ('c', 'n') and order is None:
            raise ValueError ('order must be provided for \'c\' and \'n\' modes')

        if mode == 'r':
            StreamSack.__init__ (self, open (file, 'rb', buffering = default_buffer_size), offset)
        elif mode == 'w':
            StreamSack.__init__ (self, open (file, 'r+b', buffering = default_buffer_size), offset)
        elif mode == 'c':
            if not os.path.lexists (file):
                StreamSack.__init__ (self, open (file, 'w+b', buffering = default_buffer_size),
                    offset, new = True, order = order)
                self.Flush ()
            else:
                StreamSack.__init__ (self, open (file, 'r+b', buffering = default_buffer_size), offset)
        elif mode == 'n':
            StreamSack.__init__ (self, open (file, 'w+b', buffering = default_buffer_size),
                offset, new = True, order = order)
            self.Flush ()
        else:
            raise ValueError ('Unsupported method')

    def Close (self, flush = True):
        if self.mode != 'r' and flush:
            self.Flush ()
        self.stream.close ()

# vim: nu ft=python columns=120 :
