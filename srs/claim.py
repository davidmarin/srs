#   Copyright 2014 SpendRight, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
"""Parsing/clarification of claims."""
from __future__ import unicode_literals

import re

MIXED_CLAIM_RE = re.compile(
    r'.*\b(but|however|though|'
    r'(some(\s+public)?\s+information)|'
    r'basic\s+steps)\b.*', re.I)
BAD_CLAIM_RE = re.compile(
    r'.*\b(not|unresponsive|'
    r'(no|minimal|limited|little)'
      r'(\s+public)?\s+(information|evidence|visibility)|'
    r'minimal\s+effort)\b.*', re.I)
GOOD_CLAIM_RE = re.compile(r'.*\b(distinguished)\b.*', re.I)

SENTENCE_SEP_RE = re.compile(r'(?<=\.)\s+(?=[A-Z0-9])')

def claim_to_judgment(claim, default=1):
    """General heuristics to infer a claim's judgment based on text."""
    if MIXED_CLAIM_RE.match(claim):
        return 0
    elif BAD_CLAIM_RE.match(claim):
        return -1
    elif GOOD_CLAIM_RE.match(claim):
        return 1
    else:
        return default


def clarify_claim(claim, clarifications):
    """Clarify language that only makes sense in the context of a particular
    campaign.

    Basically looks for regexes, and adds a phrase after them if it's not
    already in the claim.

    Clarifications are tuples of (regex, suffix)
    """
    for regex, suffix in clarifications:
        # make sure not already clarified. special case: don't count
        # parentheses on clarification
        look_for = suffix.lstrip('(').rstrip(')')

        if look_for.lower() in claim.lower():
            continue

        m = regex.search(claim)
        if m:
            claim = claim[:m.end()] + ' ' + suffix + claim[m.end():]

    return claim


def split_into_sentences(claim):
    return [part for part in SENTENCE_SEP_RE.split(claim) if part]


def ltrim_sentence(sentence, initial_phrases):
    """If sentence starts with one of the phrases in *initial_phrases*
    (case insensitive), trim that phrase off and then capitalize the next
    phrase.
    """
    if isinstance(initial_phrases, basestring):
        raise TypeError('initial_phrases should be a sequence, not a string')

    sentence = sentence.lstrip()

    for phrase in initial_phrases:
        if sentence.lower().startswith(phrase.lower() + ' '):
            i = len(phrase) + 1

            return (sentence[i:i + 1].upper() + sentence[i + 1:]).lstrip()

    return sentence
