# -*- coding: utf-8 -*-
import io
import struct

from .alloc import *

__all__ = ('Sack',)
#------------------------------------------------------------------------------#
# Sack                                                                         #
#------------------------------------------------------------------------------#
class Sack (object):
    """Sack

    Container which is capable storing data of arbitrary size in seekable a stream
    """
    def __init__ (self, stream, offset = 0, new = False):
        self.stream = stream
        self.offset = offset + SackDesc.size
        self.header = struct.Struct ('I')

        if not new:
            # find allocator dump
            stream.seek (offset)
            self.alloc_desc = SackDesc.FromStream (stream)
            self.alloc = BuddyAllocator.Restore (io.BytesIO (self.Get (self.alloc_desc)))

    @staticmethod
    def Create (stream, order, offset = 0):
        sack = Sack (stream, offset, new = True)
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
            if len (data) + self.header.size  <= desc.Capacity:
                self.stream.seek (self.offset + desc.Offset)
                self.stream.write (self.header.pack (len (data)))
                self.stream.write (data)
                return desc
            self.alloc.Free (desc.Offset, desc.Order)

        # allocate new block
        offset, order = self.alloc.Alloc (len (data) + self.header.size)
        try:
            self.stream.seek (self.offset + offset)
            self.stream.write (self.header.pack (len (data)))
            self.stream.write (data)

            return SackDesc (offset, order)
        except Exception:
            self.alloc.Free (offset, order)
            raise

    def Reserve (self, size, desc = None):
        """Allocate block without writing anything

        size: size of a space to be allocated
        returns: space descriptor
        """
        if desc and size + self.header.size <= desc.Capacity:
            return desc

        return SackDesc (*self.alloc.Alloc (size + self.header.size))

    def Get (self, desc):
        """Get data

        desc: data's descriptor
        returns: data
        """
        self.stream.seek (self.offset + desc.Offset)
        return self.stream.read (self.header.unpack (self.stream.read (self.header.size)) [0])

    def Pop (self, desc):
        """Pop data

        desc: data's descriptor
        returns: data
        """
        self.stream.seek (self.offset + desc.Offset)
        size =  self.header.unpack (self.stream.read (self.header.size)) [0]
        self.alloc.Free (desc.Offset, desc.Order)
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
        self.stream.seek (self.offset - SackDesc.size)
        self.stream.write (self.alloc_desc.ToBytes ())

    # context manager
    def __enter__ (self):
        return self

    def __exit__ (self, et, eo, tb):
        if et is None:
            self.Flush ()
        return False

#------------------------------------------------------------------------------#
# Sack Data Descriptor                                                         #
#------------------------------------------------------------------------------#
class SackDesc (tuple):
    __slots__ = []
    size = struct.calcsize ('QB')

    def __new__ (cls, offset, uid):
        return tuple.__new__ (cls, (offset, uid))

    # properties
    @property
    def Offset (self):
        return self [0]

    @property
    def Order (self):
        return self [1]

    @property
    def Capacity (self):
        return 1 << self [1]

    # save restore
    def ToBytes (self):
        return struct.pack ('QB', *self)

    @classmethod
    def FromBytes (cls, data):
        return cls (*struct.unpack_from ('QB', data))

    @staticmethod
    def FromStream (stream):
        return SackDesc.FromBytes (stream.read (struct.calcsize ('QB')))

    # repr
    def __str__ (self):
        return '|{0}, {1}|'.format (*self)
    __repr__ = __str__

# vim: nu ft=python columns=120 :
