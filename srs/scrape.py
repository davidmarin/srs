# -*- coding: utf-8 -*-
"""Scrape Twitter handles, facebook URLs, etc. out of a page.
"""
import re

FACEBOOK_URL_RE = re.compile(
    r'^https?://(www\.)facebook\.com/(([\w-]+)|pages/[\w-]+/\d+)/?$', re.I)

TWITTER_URL_RE = re.compile(r'^https?://(www\.)?twitter\.com/(\w+)/?$', re.I)
TWITTER_FALSE_POSITIVES = {'share'}


def scrape_copyright(soup, required=True):
    """Quick and dirty copyright notice scraper."""
    for s in soup.stripped_strings:
        if s.startswith(u'©'):
            return s

    if required:
        raise ValueError('Copyright notice not found!')


def scrape_facebook_url(soup, required=True):
    """Find twitter handle on page."""
    for a in soup.findAll('a'):
        url = a.get('href')
        if url and FACEBOOK_URL_RE.match(url):
            # normalize url scheme; Facebook now uses HTTPS
            if url.startswith('http://'):
                url = 'https://' + url[7:]
            return url

    if required:
        raise ValueError('Facebook URL not found!')


def scrape_twitter_handle(soup, required=True):
    """Find twitter handle on page."""
    for a in soup.findAll('a'):
        m = TWITTER_URL_RE.match(a.get('href', ''))
        if m:
            # "share" isn't a twitter handle
            if m.group(2) in TWITTER_FALSE_POSITIVES:
                continue

            handle = '@' + m.group(2)
            # use capitalization of handle in text, if aviailable
            if a.text and a.text.strip().lower() == handle.lower():
                handle = a.text.strip()
            # TODO: scrape twitter page to get capitalization there
            return handle

    if required:
        raise ValueError('Twitter handle not found!')
