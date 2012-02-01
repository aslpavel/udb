# -*- coding: utf-8 -*-
import io
import struct

from .alloc import *

__all__ = ('StreamSack',)
#------------------------------------------------------------------------------#
# Stream Sack                                                                  #
#------------------------------------------------------------------------------#
class StreamSack (object):
    """Stream Sack

    Container which is capable storing data of arbitrary size in seekable a stream
    """
    def __init__ (self, stream, offset, new = False):
        self.stream = stream
        self.data_header = struct.Struct ('BI')
        self.offset = offset + struct.calcsize ('Q')

        if not new:
            # find allocator dump
            stream.seek (offset)
            self.alloc_uid = struct.unpack ('Q', stream.read (struct.calcsize ('Q'))) [0]

            # restore allocator
            state = io.BytesIO (self.Get (self.alloc_uid))
            self.alloc = BuddyAllocator.Restore (state)

    @staticmethod
    def Create (stream, offset, order):
        sack = StreamSack (stream, offset, new = True)
        sack.alloc, sack.alloc_uid = BuddyAllocator (order), None
        return sack

    def Push (self, data, uid = None):
        """Push data

        data: data to be pushed
        uid: data's previous uid if any
        returns: new data's uid
        """
        header, header_size = self.data_header, self.data_header.size

        # try to save in previous location
        if uid is not None:
            self.stream.seek (self.offset + uid)
            order, size = header.unpack (self.stream.read (header_size))
            if len (data) + header_size < (1 << order):
                self.stream.seek (self.offset + uid)
                self.stream.write (header.pack (order, len (data)))
                return uid
            self.alloc.Free (uid, order)

        # allocate new block
        uid, order = self.alloc.Alloc (len (data) + header.size)
        try:
            self.stream.seek (self.offset + uid)
            self.stream.write (header.pack (order, len (data)))
            self.stream.write (data)

            return uid
        except Exception:
            self.alloc.Free (uid, order)
            raise

    def Get (self, uid):
        """Get data

        uid: data's uid
        returns: data
        """
        self.stream.seek (self.offset + uid)
        order, size = self.data_header.unpack (self.stream.read (self.data_header.size))
        return self.stream.read (size)

    def Pop (self, uid):
        """Pop data

        uid: data's uid
        returns: data
        """
        self.stream.seek (self.offset + uid)
        order, size =  self.data_header.unpack (self.stream.read (self.data_header.size))
        self.alloc.Free (uid, order)
        return self.stream.read (size)

    def Flush (self):
        # flush allocator's state
        while True:
            state = io.BytesIO ()
            self.alloc.Save (state)
            uid = self.Push (state.getvalue (), self.alloc_uid)
            if uid == self.alloc_uid:
                self.alloc_uid = uid
                break
            self.alloc_uid = uid

        # flush allocatr's uid
        self.stream.seek (self.offset - struct.calcsize ('Q'))
        self.stream.write (struct.pack ('Q', self.alloc_uid))

    # context manager
    def __enter__ (self):
        return self

    def __exit__ (self, et, eo, tb):
        if et is None:
            self.Flush ()
        return False

# vim: nu ft=python columns=120 :
