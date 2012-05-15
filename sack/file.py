# -*- coding: utf-8 -*-
import os

from .stream import *
from .alloc import *

from ..lock import *

__all__ = ('FileSack',)
#------------------------------------------------------------------------------#
# File Sack                                                                    #
#------------------------------------------------------------------------------#
buffer_size = 1 << 16 # 64Kb
class FileSack (StreamSack):
    """File Sack

    Mode:
        'r' : Open existing sack for reading only
        'w' : Open existing sack for reading and writing
        'c' : Open existing sack for reading and writing, create if it doesn't exists
        'n' : Always create a new sack
    """
    def __init__ (self, file, mode = 'r', order = None, offset = 0):
        self.mode = mode
        if mode in ('c', 'n') and order is None:
            raise ValueError ('order must be provided for \'c\' and \'n\' modes')

        if mode == 'r':
            StreamSack.__init__ (self, self.lock_init (open (file, 'rb', buffering = buffer_size)),
                    offset, readonly = True)
        elif mode == 'w':
            StreamSack.__init__ (self, self.lock_init (open (file, 'r+b', buffering = buffer_size)), offset)
        elif mode == 'c':
            if not os.path.lexists (file):
                StreamSack.__init__ (self, self.lock_init (open (file, 'w+b', buffering = buffer_size)),
                    offset, new = True, order = order)
                self.Flush ()
            else:
                StreamSack.__init__ (self, self.lock_init (open (file, 'r+b', buffering = buffer_size)), offset)
        elif mode == 'n':
            StreamSack.__init__ (self, self.lock_init (open (file, 'w+b', buffering = buffer_size)),
                offset, new = True, order = order)
            self.Flush ()
        else:
            raise ValueError ('Unsupported self.lock_init (open mode')

    #--------------------------------------------------------------------------#
    # Locking                                                                  #
    #--------------------------------------------------------------------------#
    @property
    def ReadLock (self):
        return self.lock_shared

    @property
    def WriteLock (self):
        return self.lock_exclusive

    #--------------------------------------------------------------------------#
    # Dispose                                                                  #
    #--------------------------------------------------------------------------#
    def Dispose (self):
        StreamSack.Dispose (self)
        self.stream.close ()

    #--------------------------------------------------------------------------#
    # Privaite                                                                 #
    #--------------------------------------------------------------------------#
    def lock_init (self, stream):
        self.lock_exclusive = FileLock (stream.fileno ())
        self.lock_shared    = self.lock_exclusive.Shared ()

        return stream

# vim: nu ft=python columns=120 :
