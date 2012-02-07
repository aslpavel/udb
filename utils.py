# -*- coding: utf-8 -*-

__all__ = ('Event',)
#-----------------------------------------------------------------------------#
# Event                                                                       #
#-----------------------------------------------------------------------------#
class Event (object):
    """Simple Event implementation"""
    __slots__ = ('__handlers',)

    def __init__ (self):
        self.__handlers = set ()

    def __call__ (self, *args, **keys):
        for handler in self.__handlers:
            handler (*args, **keys)

    def __iadd__ (self, handler):
        self.__handlers.add (handler)
        return self

    def __isub__ (self, handler):
        self.__handlers.discard (handler)
        return self

# vim: nu ft=python columns=120 :
