"""Code to make it possible to write simple scrapers and leave the cleanup,
merging, and normalization of records to the harness that runs them."""
from __future__ import absolute_import

import logging
import sys
from os import listdir
from os.path import dirname

import scraperwiki

from .db import TABLE_TO_KEY_FIELDS
from .db import TABLE_TO_EXTRA_FIELDS
from .norm import TM_SYMBOLS
from .norm import merge
from .rating import DEFAULT_MIN_SCORE

log = logging.getLogger(__name__)


def clear_scraper(scraper_id, tables):
    """Clear all data from the given scraper."""
    for table in tables:
        scraperwiki.sql.execute(
            'DELETE FROM {} WHERE scraper_id = ?'.format(table), [scraper_id])


def init_tables(tables, with_scraper_id=True, execute=scraperwiki.sql.execute):
    """Initialize the given tables.

    If with_scraper_id is True (default) include a scraper_id column
    in the primary key for each table.

    Generated SQL will be passed to execute (default
    is scraperwiki.sql.execute())
    """
    for table in tables:
        key_fields = TABLE_TO_KEY_FIELDS[table]
        if with_scraper_id:
            key_fields = ['scraper_id'] + key_fields

        sql = 'CREATE TABLE IF NOT EXISTS `{}` ('.format(table)
        for k in key_fields:
            sql += '`{}` TEXT, '.format(k)
        for k, field_type in TABLE_TO_EXTRA_FIELDS.get(table) or ():
            sql += '`{}` {}, '.format(k, field_type)
        sql += 'PRIMARY KEY ({}))'.format(', '.join(key_fields))

        execute(sql)


def get_scraper_ids(package='scrapers'):
    __import__(package)
    package_dir = dirname(sys.modules[package].__file__)

    for filename in sorted(listdir(package_dir)):
        if filename.endswith('.py') and not filename.startswith('_'):
            yield filename[:-3]  # meow!


def load_scraper(scraper_id, package='scrapers'):
    module_name = package + '.' + scraper_id
    __import__(module_name)
    return sys.modules[module_name]


def add_record(table, record, t2k2r):
    """Add a record for a given table to a map from
    table -> key -> row (t2k2r). Records will be cleaned up
    and merged with other records using the same key, and
    may lead to the creation of other records.

    You will want one t2k2r per scraper, which you'll
    then store using store_records.
    """
    record = record.copy()


    # catch empty fields up front
    for key in 'company', 'brand', 'category':
        if record.get(key) == '':
            raise ValueError('empty {} field in `{}`: {}'.format(
                repr(key), table, repr(record)))

    # allow company to be a dict with company info
    if 'company' in record and isinstance(record['company'], dict):
        add_record('company', record['company'])
        record['company'] = record['company']['company']

    # allow company to be a dict with company info
    if 'brand' in record and isinstance(record['brand'], dict):
        add_record('brand', record['brand'])
        record['company'] = record['brand'].get('company', '')
        record['brand'] = record['brand']['brand']

    company = record.get('company', '')

    # allow list of brands, which can be dicts
    if 'brands' in record:
        for brand in record.pop('brands'):
            company = record['company']
            if isinstance(brand, dict):
                add_record('brand', dict(company=company, **brand))
            else:
                add_record('brand', dict(company=company, brand=brand))

    # strip tm etc. off end of brand
    if record.get('brand'):
        for c in TM_SYMBOLS:
            idx = record['brand'].find(c)
            if idx != -1:
                record['brand'] = record['brand'][:idx]
                record['tm'] = c

    # note that brand is also used in the loop above
    brand = record.get('brand', '')

    # allow single category
    if 'category' in record and not (
            table == 'category' or table.endswith('category')):
        record['categories'] = [record.pop('category')]

    # allow list of categories (strings only)
    if 'categories' in record:
        if brand:
            for c in record.pop('categories'):
                add_record('brand_category', dict(
                    company=company, brand=brand, category=c))
        else:
            for category in record.pop('categories'):
                add_record('company_category', dict(
                    company=company, category=category))

    # assume min_score of 0 if not specified
    if 'score' in record and 'min_score' not in record:
        record['min_score'] = DEFAULT_MIN_SCORE

    # automatic brand entries
    if 'brand' in record and table != 'brand':
        add_record('brand', dict(company=company, brand=brand))

    # automatic category entries
    if 'category' in record and table != 'category':
        add_record('category', dict(category=record['category']))
    if 'parent_category' in record:
        add_record('category', dict(category=record['parent_category']))

    # automatic company entries
    if 'company' in record and table != 'company':
        add_record('company', dict(company=company))

    _add_record(table, record, t2k2r)


def _add_record(table, record, t2k2r):
    """Help for add_record(). Clean up *record* and add it to
    t2k2r, merging with any existing record."""
    key_fields = TABLE_TO_KEY_FIELDS[table]

    # clean strings before storing them
    for k in record:
        if k is None:
            del record[k]
        elif isinstance(record[k], basestring):
            record[k] = clean_string(record[k])

    # verify that URLs are absolute
    for k in record:
        if k.split('_')[-1] == 'url':
            if record[k] and not urlparse(record[k]).scheme:
                raise ValueError('{} has no scheme: {}'.format(
                    k, repr(record)))

    # only scope may be empty/unset
    for k in key_fields:
        if record.get(k) in (None, ''):
            if k == 'scope':
                record[k] = ''
            else:
                raise ValueError('empty {} field for `{}`: {}'.format(
                    repr(k), table, repr(record)))

    key = tuple(record[k] for k in key_fields)

    log.debug('`{}` {}: {}'.format(table, repr(key), repr(record)))

    t2k2r.setdefault(table, {})
    k2r = t2k2r[table]

    if key in k2r:
        merge(record, k2r[key])
    else:
        k2r[key] = record
