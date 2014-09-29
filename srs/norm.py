"""Normalization and cleanup."""
import re

# matches all whitespace, including non-ascii (e.g. non-breaking space)
WHITESPACE_RE = re.compile(r'\s+', re.U)


def clean_string(s):
    """Convert to unicode, remove extra whitespace, and
    convert fancy apostrophes."""
    s = unicode(s)
    s = WHITESPACE_RE.sub(' ', s).strip()
    s = s.replace(u'\u2019', "'")  # "smart" apostrophe
    return s
