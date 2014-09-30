"""Utilities for deailing with ratings."""

# 0 is a really common minimum score
DEFAULT_MIN_SCORE = 0


def grade_to_judgment(grade):
    """Convert a letter grade (e.g. "B+") to a judgment (1 for A or B,
    0 for C, -1 for D, E, or F).

    This works for Free2Work and Rank a Brand, anyways. In theory, campaigns
    could color their grades differently.
    """
    return cmp('C', grade[0].upper())
