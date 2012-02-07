# -*- coding: utf-8 -*-
import os
import struct
import contextlib

from .utils import *
from .bptree import *
from .sack import *
from .providers.sack import *

__all__ = ('uDB',)

#------------------------------------------------------------------------------#
# Default Values                                                               #
#------------------------------------------------------------------------------#
default_buffer_size  = 1 << 16 # 65536
default_sack_order   = 32      # 4GB
default_bptree_order = 32

#------------------------------------------------------------------------------#
# Micro Database Interface                                                     #
#------------------------------------------------------------------------------#
class uDB (BPTree):
    def __init__ (self, stream, close = False):
        stream.seek (0)
        provider_desc = struct.unpack ('>Q', stream.read (struct.calcsize ('>Q'))) [0]
        BPTree.__init__ (self, SackProvider (Sack (stream, struct.calcsize ('>Q')), provider_desc))

        # fields
        self.stream = stream
        self.OnOpen = Event ()
        self.OnClose = Event ()

        if close:
            self.OnClose += lambda: self.stream.close ()

    # create
    @classmethod
    def Create (cls, stream, order = None, capacity_order = None, close = False,  **keys):
        """Create new database"""
        # create sack (default 4GB)
        sack = Sack.Create (stream, capacity_order if capacity_order else default_sack_order,
            struct.calcsize ('>Q'))

        # create provider (with default order 32)
        provider = SackProvider.Create (sack, order if order else default_bptree_order)
        stream.seek (0)
        stream.write (struct.pack ('>Q', provider.desc))
        provider.Flush ()

        return cls (stream, close)

    @classmethod
    def Open (cls, path, mode = 'r', order = None, capacity_order = None):
        """Open database

        path: path to the file containing database
        mode:
            'r' : Open existing database for reading only
            'w' : Open existing database for reading and writing
            'c' : Open existing database for reading and writing, create if it doesn't exists
            'n' : Always create a new database
        """
        if mode in ('r', 'w'):
            return cls (open (path, 'rb' if mode == 'r' else 'r+b', buffering = default_buffer_size), close = True)

        elif mode == 'c':
            try:
                return cls (open (path, 'r+b', buffering = default_buffer_size), close = True)
            except IOError: pass
            return cls.Create (open (path, 'w+b', buffering = default_buffer_size),
                order, capacity_order, close = True)

        elif mode == 'n':
            return cls.Create (open (path, 'w+b', buffering = default_buffer_size),
                order, capacity_order, close = True)

        raise ValueError ('mode \'{0}\' is not available'.format (mode))

    def Flush (self):
        self.provider.Flush ()
        self.stream.flush ()

    # make transaction
    @contextlib.contextmanager
    def Transaction (self):
        yield
        self.provider.Flush ()

    def __enter__ (self):
        self.OnOpen ()
        return self

    def __exit__ (self, et, eo, tb):
        self.OnClose ()
        return False

# vim: nu ft=python columns=120 :
