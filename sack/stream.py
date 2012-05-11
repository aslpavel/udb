# -*- coding: utf-8 -*-
import struct

from .sack import *

__all__ = ('StreamSack',)
#------------------------------------------------------------------------------#
# Stream Sack                                                                  #
#------------------------------------------------------------------------------#
class StreamSack (Sack):
    """Stream Sack

    Container which is capable storing data of arbitrary size in seekable a stream
    and associate it with unique descriptor
    Descriptor Format:
        0        7       8
        +--------+-------+
        | offset | order |
        +--------+-------+
    """
    def __init__ (self, stream, offset = 0, order = None, new = False, readonly = None):
        # headers
        self.data_header = struct.Struct ('!I')
        self.header = struct.Struct ('!QQ')

        self.stream = stream
        self.offset = offset
        self.data_offset = offset + self.header.size

        if new:
            Sack.__init__ (self, None, None, order, readonly = readonly)
        else:
            with self.ReadLock:
                stream.seek (offset)
                alloc_desc, cell_desc = self.header.unpack (stream.read (self.header.size))
            Sack.__init__ (self, cell_desc, alloc_desc, readonly = readonly)

    #--------------------------------------------------------------------------#
    # Access Data                                                              #
    #--------------------------------------------------------------------------#
    def Push (self, data, desc = None):
        """Push data

        data: data to be pushed
        desc: data's previous descriptor if any
        returns: new data's descriptor
        """
        with self.WriteLock:
            # try to save in previous location
            if desc is not None:
                if len (data) + self.data_header.size  <= 1 << (desc & 0xff): # desc.capacity
                    self.stream.seek (self.data_offset + (desc >> 8)) # desc.offset
                    self.stream.write (self.data_header.pack (len (data)))
                    self.stream.write (data)
                    return desc
                self.alloc.Free (desc >> 8, desc & 0xff) # desc.offset, desc.order

            # allocate new block
            offset, order = self.alloc.Alloc (len (data) + self.data_header.size)
            self.resize (self.data_offset + offset + (1 << order))
            try:
                self.stream.seek (self.data_offset + offset)
                self.stream.write (self.data_header.pack (len (data)))
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
            if size + self.data_header.size <= 1 << (desc & 0xff): # desc.capacity
                return desc
            self.alloc.Free (desc >> 8, desc & 0xff)

        offset, order = self.alloc.Alloc (size + self.data_header.size)
        self.resize (self.data_offset + offset + (1 << order))
        return order | offset << 8 # desc

    def Get (self, desc):
        """Get data

        desc: data's descriptor
        returns: data
        """
        with self.ReadLock:
            self.stream.seek (self.data_offset + (desc >> 8)) # desc.offset
            return self.stream.read (self.data_header.unpack (self.stream.read (self.data_header.size)) [0])

    def Pop (self, desc):
        """Pop data

        desc: data's descriptor
        returns: data
        """
        with self.WriteLock:
            offset, order = desc >> 8, desc & 0xff
            self.stream.seek (self.data_offset + offset)
            self.alloc.Free (offset, order)
            return self.stream.read (self.data_header.unpack (self.stream.read (self.data_header.size)) [0])

    #--------------------------------------------------------------------------#
    # Flush                                                                    #
    #--------------------------------------------------------------------------#
    def Flush (self):
        with self.WriteLock:
            Sack.Flush (self)

            # flush header
            self.stream.seek (self.offset)
            self.stream.write (self.header.pack (self.alloc_desc, self.Cell.desc))
            self.stream.flush ()

    #--------------------------------------------------------------------------#
    # private                                                                  #
    #--------------------------------------------------------------------------#
    def resize (self, size):
        """Resize stream if needed

        This hook is used by mmap
        """
        pass

# vim: nu ft=python columns=120 :
