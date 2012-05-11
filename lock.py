# -*- coding: utf-8 -*-

__all__ = ('Lock', 'DummyLock', 'FileLock', 'LockError',)
#------------------------------------------------------------------------------#
# Exceptions                                                                   #
#------------------------------------------------------------------------------#
class LockError (Exception): pass

#------------------------------------------------------------------------------#
# Base Lock                                                                    #
#------------------------------------------------------------------------------#
class BaseLock (object):
    __slots__ = tuple ()
    #--------------------------------------------------------------------------#
    # State                                                                    #
    #--------------------------------------------------------------------------#
    def IsLocked (self):
        raise NotImplementedError ()

    def __bool__ (self):
        return self.IsLocked ()

    def __nonzero__ (self):
        return self.IsLocked ()

    #--------------------------------------------------------------------------#
    # Acquire | Release                                                        #
    #--------------------------------------------------------------------------#
    def Acquire (self):
        raise NotImplementedError ()

    def Release (self):
        raise NotImplementedError ()
    
    #--------------------------------------------------------------------------#
    # Scope                                                                    #
    #--------------------------------------------------------------------------#
    def __enter__ (self):
        self.Acquire ()
        return self

    def __exit__ (self, et, eo, tb):
        self.Release ()
        return False

#------------------------------------------------------------------------------#
# Lock                                                                         #
#------------------------------------------------------------------------------#
class Lock (BaseLock):
    __slots__ = ('locked',)

    def __init__ (self):
        self.locked = False

    #--------------------------------------------------------------------------#
    # Lock Interface                                                           #
    #--------------------------------------------------------------------------#
    def Acquire (self):
        if self.locked:
            raise LockError ('Lock has already been acquired')
        self.locked = True

    def Release (self):
        if not self.locked:
            raise LockError ('Lock is not acquired')
        self.locked = False

    def IsLocked (self):
        return self.locked

#------------------------------------------------------------------------------#
# Dummy Lock                                                                   #
#------------------------------------------------------------------------------#
class DummyLock (BaseLock):
    __slots__ = ('locked',)

    def __init__ (self):
        self.locked = 0

    #--------------------------------------------------------------------------#
    # Lock Interface                                                           #
    #--------------------------------------------------------------------------#
    def Acquire (self):
        self.locked += 1

    def Release (self):
        if self.locked > 0:
            self.locked -= 1

    def IsLocked (self):
        return self.locked

#------------------------------------------------------------------------------#
# File Lock                                                                    #
#------------------------------------------------------------------------------#
try:
    from fcntl import flock, LOCK_SH, LOCK_EX, LOCK_NB, LOCK_UN
except ImportError:
    LOCK_SH = 1
    LOCK_EX = 2
    LOCK_NB = 4
    LOCK_UN = 8

    def flock (fd, opt):
        pass

class FileLock (BaseLock):
    __slots__ = ('fd', 'exclusive', 'locked')

    def __init__ (self, fd, exclusive = True):
        self.fd        = fd
        self.exclusive = exclusive
        self.locked    = 0

    #--------------------------------------------------------------------------#
    # Lock Interface                                                           #
    #--------------------------------------------------------------------------#
    def Acquire (self, mode = None):
        if mode is None:
            mode = LOCK_EX if self.exclusive else LOCK_SH
        flock (self.fd, mode)
        self.locked += 1

    def Release (self):
        if self.locked > 0:
            if self.locked == 1:
                flock (self.fd, LOCK_UN)
            self.locked -= 1

    def IsLocked (self):
        return self.locked > 0

    #--------------------------------------------------------------------------#
    # Shared | Exclusive                                                       #
    #--------------------------------------------------------------------------#
    def Shared (self):
        if not self.exclusive:
            return self
        return SubjectFileLock (self, not self.exclusive)

    def Exclusive (self):
        if self.exclusive:
            return self
        return SubjectFileLock (self, not self.exclusive)

class SubjectFileLock (BaseLock):
    __slots__ = ('lock', 'exclusive',)

    def __init__ (self, lock, exclusive): 
        self.lock = lock
        self.exclusive = exclusive

    #--------------------------------------------------------------------------#
    # Lock Interface                                                           #
    #--------------------------------------------------------------------------#
    def Acquire (self):
        self.lock.Acquire (LOCK_EX if self.exclusive else LOCK_SH)

    def Release (self):
        self.lock.Release ()

    def IsLocked (self):
        return self.lock.IsLocked ()

    #--------------------------------------------------------------------------#
    # Shared | Exclusive                                                       #
    #--------------------------------------------------------------------------#
    def Shared (self):
        if not self.exclusive:
            return self
        return self.lock

    def Exclusive (self):
        if self.exclusive:
            return self
        return self.lock
# vim: nu ft=python columns=120 :
