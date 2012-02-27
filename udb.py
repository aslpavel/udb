# -*- coding: utf-8 -*-
import os
import struct
import contextlib

from .utils import *
from .bptree import *
from .sack.file import *
from .providers.bytes import *

__all__ = ('uDB',)

#------------------------------------------------------------------------------#
# Default Values                                                               #
#------------------------------------------------------------------------------#
default_sack_order    = 32      # 4GB
default_bptree_order  = 32
default_provider_type = SackBytesProvider

#------------------------------------------------------------------------------#
# Micro Database Interface                                                     #
#------------------------------------------------------------------------------#
class uDB (BPTree):
    def __init__ (self, file, mode = 'r', order = None, capacity_order = None, provider_type = None, **keys):
        # init defaults
        provider_type  = default_provider_type if provider_type is None else provider_type
        capacity_order = default_sack_order if capacity_order is None else capacity_order
        order          = default_bptree_order if order is None else order

        # create sack
        self.sack = FileSack (file, mode = mode, offset = struct.calcsize ('!Q'), order = capacity_order)
        stream = self.sack.stream
        if self.sack.IsNew:
            # create new provider
            provider = provider_type.Create (self.sack, order)
            # save provider's descriptor
            stream.seek (0)
            stream.write (struct.pack ('!Q', provider.desc))
            provider.Flush ()

        stream.seek (0)
        provider_desc = struct.unpack ('!Q', stream.read (struct.calcsize ('!Q'))) [0]
        BPTree.__init__ (self, provider_type (self.sack, provider_desc))

    def Flush (self):
        self.provider.Flush ()

    # make transaction
    @contextlib.contextmanager
    def Transaction (self):
        yield
        self.provider.Flush ()

    def __enter__ (self):
        return self

    def __exit__ (self, et, eo, tb):
        self.sack.__exit__ (et, eo, tb)
        return False

# vim: nu ft=python columns=120 :