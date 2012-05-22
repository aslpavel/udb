# -*- coding: utf-8 -*-
import sys
import array
import struct

__all__ = ('BytesList', 'ArraySave', 'ArrayLoad')
#------------------------------------------------------------------------------#
# Bytes List                                                                   #
#------------------------------------------------------------------------------#
class BytesList (list):
    count_header = struct.Struct ('!I')

    def __init__ (self, items = tuple ()):
        list.__init__ (self, items)

    #--------------------------------------------------------------------------#
    # Save | Load                                                              #
    #--------------------------------------------------------------------------#
    def Save (self, stream):
        # header (count, sizes)
        stream.write (self.count_header.pack (len (self)))
        ArraySave (stream, array.array ('i', ((-1 if item is None else len (item)) for item in self)))

        # items
        for item in self:
            if item is not None:
                stream.write (item)

    @classmethod
    def Load (cls, stream):
        sizes = ArrayLoad (stream, 'i', cls.count_header.unpack (stream.read (cls.count_header.size)) [0])
        return cls ((stream.read (size) if size >= 0 else None) for size in sizes)

#------------------------------------------------------------------------------#
# Array (Save|Load)                                                            #
#------------------------------------------------------------------------------#
if sys.version_info [0] < 3:
# Python 2
    def ArraySave (stream, array):
        stream.write (array.tostring ())

    def ArrayLoad (stream, type, count):
        sizes = array.array (type)
        sizes.fromstring (stream.read (sizes.itemsize * count))
        return sizes
else:
    def ArraySave (stream, array):
        array.tofile (stream)

    def ArrayLoad (stream, type, count):
        sizes = array.array (type)
        sizes.fromfile (stream, count)
        return sizes
# vim: nu ft=python columns=120 :
