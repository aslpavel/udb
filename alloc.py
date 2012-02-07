# -*- coding: utf-8 -*-
import io
import struct

from array import array
from bisect import insort, bisect_left

__all__ = ('BuddyAllocator', 'AllocatorError')

#------------------------------------------------------------------------------#
# Buddy Allocator                                                              #
#------------------------------------------------------------------------------#
class BuddyAllocator (object):
    def __init__ (self, order, map = None):
        """Initialize

        order: allocator's order (size = 1 << order)
        map: allocator's  free blocks map
        """
        self.order = order

        # initialize map
        if map is None:
            self.map = [array ('L') for k in range (order + 1)]
            self.map [order].append (0)
        else:
            self.map = map

    def AllocOrder (self, order):
        """Allocate block

        order: block's order
        returns: block's offset
        """
        map_order = order
        # find not empty map of sufficient size
        while map_order <= self.order:
            map = self.map [map_order]
            if not map:
                map_order += 1
                continue
            block = map.pop (0)

            # split block until best-fit is found
            while map_order > order:
                map_order -= 1
                insort (self.map [map_order], block + (1 << map_order))

            return block

        raise AllocatorError ('out of space')

    def Alloc (self, size):
        """Allocate block

        size: size of block
        returns: (offset, order)
        """
        order = int.bit_length (size - 1)
        return self.AllocOrder (order), order

    def Free (self, offset, order):
        """Free block

        offset: block's offset
        order:  block's order
        """
        while order < self.order:
            buddy_offset = offset ^ (1 << order)
            map = self.map [order]

            # check if buddy is in a free map
            buddy_index = bisect_left (map, buddy_offset)
            if not map or buddy_index >= len (map) or buddy_offset != map [buddy_index]:
                    map.insert (buddy_index, offset)
                    return

            # merge with buddy
            map.pop (buddy_index)
            if offset & (1 << order):
                offset = buddy_offset
            order += 1

        self.map [order].append (offset)

    @property
    def UsedSpace (self):
        used = 0
        for order, map in enumerate (self.map):
            used += len (map) * (1 << order)
        return (1 << self.order) - used

    def Save (self, stream):
        """Save allocator state to stream"""
        stream.write (struct.pack ('B', self.order))
        array ('I', (len (map) for map in self.map)).tofile (stream)
        for map in self.map:
            map.tofile (stream)

    @staticmethod
    def Restore (stream):
        """Restore allocator state from stream"""
        order = struct.unpack ('B', stream.read (struct.calcsize ('B'))) [0]

        sizes = array ('I')
        sizes.fromfile (stream, order + 1)

        map = [array ('L') for k in range (order + 1)]
        for k, size in enumerate (sizes):
            map [k].fromfile (stream, size)

        return BuddyAllocator (order, map)

class AllocatorError (Exception): pass
# vim: nu ft=python columns=120 :
