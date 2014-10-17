"""Utilities for setting up logging."""
from __future__ import absolute_import

import logging
from os import environ

DEFAULT_FORMAT = '%(name)s: %(message)s'


def log_to_stderr(verbose=False, quiet=False, format=DEFAULT_FORMAT):
    """Set up logging to stderr."""
    level = logging.INFO
    if verbose or environ.get('MORPH_VERBOSE'):
        level = logging.DEBUG
    elif quiet:
        level = logging.WARN

    logging.basicConfig(format=format, level=level)
    if not verbose:
        logging.getLogger('requests').setLevel(logging.WARN)
