# -*- coding: utf-8 -*-
"""Scrape Twitter handles, facebook URLs, etc. out of a page.
"""
import json
import re
from logging import getLogger
from os import rename
from tempfile import NamedTemporaryFile
from time import sleep
from urllib2 import Request
from urllib2 import urlopen
from urllib2 import URLError

from bs4 import BeautifulSoup

from .vendor.reppy.cache import RobotsCache

DEFAULT_HEADERS = {
    'Accept': 'text/html',
    'User-Agent': 'Mozilla/5.0',
}

DEFAULT_TIMEOUT = 30

CHUNK_SIZE = 1024  # for download()


FACEBOOK_URL_RE = re.compile(
    r'^https?://(www\.)facebook\.com/(([\w-]+)|pages/[\w-]+/\d+)/?$', re.I)

TWITTER_URL_RE = re.compile(r'^https?://(www\.)?twitter\.com/(\w+)/?$', re.I)
TWITTER_FALSE_POSITIVES = {'share'}

ROBOTS = RobotsCache(timeout=DEFAULT_TIMEOUT)

log = getLogger(__name__)


class DisallowedByRobotsTxtError(URLError):
    def __init__(self):
        super(DisallowedByRobotsTxtError, self).__init__(
            'disallowed by robots.txt')


def download(url, dest):
    """Download url to the given path, moving it into place when done."""
    with NamedTemporaryFile(prefix=dest + '.tmp.', dir='.', delete=False) as f:
        src = urlopen(url)
        while True:
            chunk = src.read(CHUNK_SIZE)
            if not chunk:
                break
            f.write(chunk)

        f.close()
        rename(f.name, dest)


def scrape(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT,
           ignore_robots_txt=False, data=None):
    """Return the bytes from the given page, respecting robots.txt
    by default.

    To do a POST request, set data to urlencoded query params
    (same as data argument to urlopen()).
    """
    if headers is None:
        headers=DEFAULT_HEADERS

    if not ignore_robots_txt:
        user_agent = headers.get('User-Agent', '')

        if not ROBOTS.allowed(url, user_agent):
            raise DisallowedByRobotsTxtError()

        crawl_delay = ROBOTS.delay(url, user_agent)
        if crawl_delay:
            log.debug('sleeping for {:.1f} seconds (crawl-delay)'.format(
                crawl_delay))
            sleep(crawl_delay)

    return urlopen(
        Request(url, headers=headers), data=data, timeout=timeout).read()


def scrape_json(url, **kwargs):
    return json.loads(scrape(url, **kwargs))


def scrape_soup(url, **kwargs):
    """Scrape the given page, and convert to BeautifulSoup."""
    html = scrape(url, **kwargs)

    # assume utf8 (sometimes BeautifulSoup fails to do this)
    try:
        html = html.decode('utf8')
    except UnicodeDecodeError:
        pass

    return BeautifulSoup(html)


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
