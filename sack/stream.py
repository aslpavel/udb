# -*- coding: utf-8 -*-
import io
import struct

from .alloc import *
from ..utils import *

__all__ = ('StreamSack',)
#------------------------------------------------------------------------------#
# Stream Sack                                                                  #
#------------------------------------------------------------------------------#
class StreamSack (object):
    """Stream Sack

    Container which is capable storing data of arbitrary size in seekable a stream
    and associate it with unique descriptor
    Descriptor Format:
        0        7       8
        +--------+-------+
        | offset | order |
        +--------+-------+
    """
    def __init__ (self, stream, offset = 0, new = False):
        self.stream = stream
        self.offset = offset
        self.data_offset = offset + 8 # desc.size
        self.header = struct.Struct ('I')

        self.IsNew = new
        if not new:
            # find allocator dump
            stream.seek (offset)
            self.alloc_desc = struct.unpack ('Q', stream.read (8)) [0] # desc.from_stream
            self.alloc = BuddyAllocator.Restore (io.BytesIO (self.Get (self.alloc_desc)))

        self.OnFlush = Event ()

    @classmethod
    def Create (cls, stream, order, offset = 0):
        sack = cls (stream, offset, new = True)
        sack.alloc, sack.alloc_desc = BuddyAllocator (order), None
        return sack

    def Push (self, data, desc = None):
        """Push data

        data: data to be pushed
        desc: data's previous descriptor if any
        returns: new data's descriptor
        """
        # try to save in previous location
        if desc is not None:
            if len (data) + self.header.size  <= 1 << (desc & 0xff): # desc.capacity
                self.stream.seek (self.data_offset + (desc >> 8)) # desc.offset
                self.stream.write (self.header.pack (len (data)))
                self.stream.write (data)
                return desc
            self.alloc.Free (desc >> 8, desc & 0xff) # desc.offset, desc.order

        # allocate new block
        offset, order = self.alloc.Alloc (len (data) + self.header.size)
        try:
            self.stream.seek (self.data_offset + offset)
            self.stream.write (self.header.pack (len (data)))
            self.stream.write (data)

            return order | offset << 8 # desc
        except Exception:
            self.alloc.Free (offset, order)
            raise

    def Reserve (self, size, desc = None):
        """Allocate block without writing anything

        size: size of a space to be allocated
        returns: space descriptor
        """
        if desc:
            if size + self.header.size <= 1 << (desc & 0xff): # desc.capacity
                return desc
            self.alloc.Free (desc >> 8, desc & 0xff)

        offset, order = self.alloc.Alloc (size + self.header.size)
        return order | offset << 8 # desc

    def Get (self, desc):
        """Get data

        desc: data's descriptor
        returns: data
        """
        self.stream.seek (self.data_offset + (desc >> 8)) # desc.offset
        return self.stream.read (self.header.unpack (self.stream.read (self.header.size)) [0])

    def Pop (self, desc):
        """Pop data

        desc: data's descriptor
        returns: data
        """
        offset, order = desc >> 8, desc & 0xff
        self.stream.seek (self.data_offset + offset)
        size =  self.header.unpack (self.stream.read (self.header.size)) [0]
        self.alloc.Free (offset, order)
        return self.stream.read (size)

    def Flush (self):
        # flush allocator's state
        while True:
            state = io.BytesIO ()
            self.alloc.Save (state)
            desc = self.Push (state.getvalue (), self.alloc_desc)
            if desc == self.alloc_desc:
                self.alloc_desc = desc
                break
            self.alloc_desc = desc

        # flush allocator's desc
        self.stream.seek (self.offset) # desc.size
        self.stream.write (struct.pack ('Q', self.alloc_desc))

        # fire event
        self.OnFlush (self)

    # context manager
    def __enter__ (self):
        return self

    def __exit__ (self, et, eo, tb):
        if et is None:
            self.Flush ()
        return False

# vim: nu ft=python columns=120 :
