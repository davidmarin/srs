"""Utilities for dealing with ISO datetime strings."""
from __future__ import absolute_import

from datetime import date
from datetime import datetime

ISO_8601_FMT = '%Y-%m-%dT%H:%M:%S.%fZ'

ISO_8601_DATE_FMT = '%Y-%m-%d'


def iso_now():
    """Get the current (UTC) time as an ISO datetime."""
    return datetime.utcnow().strftime(ISO_8601_FMT)


def iso_today():
    """Get the current (UTC) time as an ISO date."""
    return datetime.utcnow().strftime(ISO_8601_DATE_FMT)


def to_iso(utc_dt):
    """Convert a (UTC) datetime to an ISO datetime."""
    return utc_dt.strftime(ISO_8601_FMT)


def to_iso_date(utc_date):
    """Convert a date or datetime to an ISO date."""
    return utc_date.strftime(ISO_8601_DATE_FMT)


def from_iso(iso_dt):
    """Convert an ISO datetime to a (UTC) datetime object."""
    return datetime.strptime(iso_dt, ISO_8601_FMT)


def from_iso_date(iso_date):
    """Convert an ISO date to a date object)."""
    return date.strptime(iso_date, ISO_8601_DATE_FMT)
