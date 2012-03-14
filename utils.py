# -*- coding: utf-8 -*-
import array
import struct

__all__ = ('BytesList',)
#------------------------------------------------------------------------------#
# Bytes List                                                                   #
#------------------------------------------------------------------------------#
class BytesList (list):
    count_header = struct.Struct ('!I')

    def __init__ (self, items = tuple ()):
        list.__init__ (self, items)

    def Save (self, stream):
        # item count
        stream.write (self.count_header.pack (len (self)))
        # item sizes
        sizes = array.array ('i', ((-1 if item is None else len (item)) for item in self))
        stream.write (sizes.tostring ())
        # items
        for item in self:
            if item is not None:
                stream.write (item)

    @classmethod
    def Load (cls, stream):
        # item count
        count = cls.count_header.unpack (stream.read (cls.count_header.size)) [0]
        # item sizes
        sizes = array.array ('i')
        sizes.fromstring (stream.read (sizes.itemsize * count))
        # items
        return cls ((stream.read (size) if size >= 0 else None) for size in sizes)

# vim: nu ft=python columns=120 :
