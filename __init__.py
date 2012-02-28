# -*- coding: utf-8 -*-
from .udb import uDB
from .providers.pickle import *
from .providers.bytes import *

__all__ = ('uDB', 'PickleProvider', 'BytesProvider')

#------------------------------------------------------------------------------#
# Load Tests Protocol                                                          #
#------------------------------------------------------------------------------#
def load_tests (loader, tests, pattern):
    from . import tests
    return loader.loadTestsFromModule (tests)
# vim: nu ft=python columns=120 :
