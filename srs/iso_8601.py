"""Utilities for dealing with ISO datetime strings."""
from __future__ import absolute_import

from datetime import datetime

ISO_8601_FMT = '%Y-%m-%dT%H:%M:%S.%fZ'


def iso_now():
    """Get the current (UTC) time as an ISO datetime."""
    return datetime.utcnow().strftime(ISO_8601_FMT)


def to_iso(utc_dt):
    """Convert a (UTC) datetime to an ISO datetime."""
    return utc_dt.strftime(ISO_8601_FMT)


def from_iso(iso_dt):
    """Convert an ISO datetime to a (UTC) datetime object."""
    return datetime.strptime(iso_dt, ISO_8601_FMT)
