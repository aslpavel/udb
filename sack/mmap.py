# -*- coding: utf-8 -*-
import os
import mmap
import struct

from .stream import *

__all__ = ('MMapSack',)
#------------------------------------------------------------------------------#
# Memory Map Sack                                                              #
#------------------------------------------------------------------------------#
class MMapSack (StreamSack):
    """Memory Map based Sack

    Mode:
        'r' : Open existing sack for reading only
        'w' : Open existing sack for reading and writing
        'c' : Open existing sack for reading and writing, create if it doesn't exists
        'n' : Always create a new sack
    """
    def __init__ (self, file, mode = 'r', offset = 0, order = None):
        # headers
        self.data_header = struct.Struct ('!I')
        self.header = struct.Struct ('!QQ')

        # open file
        prot = mmap.PROT_READ | mmap.PROT_WRITE
        readonly = False
        is_new = False
        if mode == 'r':
            prot, fd = mmap.PROT_READ, os.open (file, os.O_RDONLY)
            readonly = True
        elif mode == 'w':
            fd = os.open (file, os.O_RDWR)
        elif mode == 'c':
            is_new, fd = not os.path.lexists (file), os.open (file, os.O_RDWR | os.O_CREAT)
        elif mode == 'n':
            is_new, fd = True, os.open (file, os.O_RDWR | os.O_TRUNC)
        else:
            raise ValueError ('Unsupported open mode')

        # create mmap
        if is_new:
            os.write (fd, b'\x00' * self.header.size)
        stream = mmap.mmap (fd, 0, mmap.MAP_SHARED, prot)

        StreamSack.__init__ (self, stream, offset, order, is_new, readonly = readonly)

    #--------------------------------------------------------------------------#
    # Dispose                                                                  #
    #--------------------------------------------------------------------------#
    def Dispose (self):
        StreamSack.Dispose (self)
        self.stream.close ()

    #--------------------------------------------------------------------------#
    # Private                                                                  #
    #--------------------------------------------------------------------------#
    def resize (self, size):
        if len (self.stream) < size:
            self.stream.resize (size)
# vim: nu ft=python columns=120 :
