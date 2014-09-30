"""Code to make it possible to write simple scrapers and leave the cleanup,
merging, and normalization of records to the harness that runs them."""
from __future__ import absolute_import

import logging
import sys
from datetime import datetime
from os import listdir
from os.path import dirname
from urlparse import urlparse

import scraperwiki

from .db import TABLE_TO_KEY_FIELDS
from .db import TABLE_TO_EXTRA_FIELDS
from .norm import TM_SYMBOLS
from .norm import clean_string
from .norm import merge
from .rating import DEFAULT_MIN_SCORE

ISO_8601_FMT = '%Y-%m-%dT%H:%M:%S.%fZ'

log = logging.getLogger(__name__)


def delete_records_from_scraper(scraper_id, tables):
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


def add_record(table, record, table_to_key_to_row):
    """Add a record for a given table to a map from
    table -> key -> row (table_to_key_to_row). Records will be cleaned up
    and merged with other records using the same key, and
    may lead to the creation of other records.

    You will want one table_to_key_to_row per scraper, which you'll
    then store using store_records.
    """
    # recursively add a record, possibly creating other records
    def _add(table, record):
        record = record.copy()

        # catch empty fields up front
        for key in 'company', 'brand', 'category':
            if record.get(key) == '':
                raise ValueError('empty {} field in `{}`: {}'.format(
                    repr(key), table, repr(record)))

        # allow company to be a dict with company info
        if 'company' in record and isinstance(record['company'], dict):
            _add('company', record['company'])
            record['company'] = record['company']['company']

        # allow company to be a dict with company info
        if 'brand' in record and isinstance(record['brand'], dict):
            _add('brand', record['brand'])
            record['company'] = record['brand'].get('company', '')
            record['brand'] = record['brand']['brand']

        company = record.get('company', '')

        # allow list of brands, which can be dicts
        if 'brands' in record:
            for brand in record.pop('brands'):
                company = record['company']
                if isinstance(brand, dict):
                    _add('brand', dict(company=company, **brand))
                else:
                    _add('brand', dict(company=company, brand=brand))

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
                    _add('brand_category', dict(
                        company=company, brand=brand, category=c))
            else:
                for category in record.pop('categories'):
                    _add('company_category', dict(
                        company=company, category=category))

        # assume min_score of 0 if not specified
        if 'score' in record and 'min_score' not in record:
            record['min_score'] = DEFAULT_MIN_SCORE

        # automatic brand entries
        if 'brand' in record and table != 'brand':
            _add('brand', dict(company=company, brand=brand))

        # automatic category entries
        if 'category' in record and table != 'category':
            _add('category', dict(category=record['category']))
        if 'parent_category' in record:
            _add('category', dict(category=record['parent_category']))

        # automatic company entries
        if 'company' in record and table != 'company':
            _add('company', dict(company=company))

        # actually clean up and store/merge the record
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

        table_to_key_to_row.setdefault(table, {})
        key_to_row = table_to_key_to_row[table]

        if key in key_to_row:
            merge(record, key_to_row[key])
        else:
            key_to_row[key] = record

    _add(table, record)


def save_records_from_scraper(records, scraper_id, supported_tables=None):
    if supported_tables is None:
        supported_tables = set(TABLE_TO_KEY_FIELDS)

    table_to_key_to_row = {}

    for table, record in records:
        if table not in supported_tables:
            raise ValueError('unsupported table `{}`'.format(table))
        add_record(table, record, table_to_key_to_row)

    # add the time this campaign was scraped
    add_record('scraper',
               dict(last_scraped=datetime.utcnow().strftime(ISO_8601_FMT)),
               table_to_key_to_row)

    delete_records_from_scraper(scraper_id, supported_tables)

    for table in table_to_key_to_row:
        key_fields = TABLE_TO_KEY_FIELDS[table]

        for key, row in table_to_key_to_row[table].iteritems():
            scraperwiki.sql.save(
                ['scraper_id'] + key_fields,
                dict(scraper_id=scraper_id, **row),
                table_name=table)
