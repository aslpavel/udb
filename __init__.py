# -*- coding: utf-8 -*-

#------------------------------------------------------------------------------#
# Load Tests Protocol                                                          #
#------------------------------------------------------------------------------#
def load_tests (loader, tests, pattern):
    from . import tests
    return loader.loadTestsFromModule (tests)
# vim: nu ft=python columns=120 :