"""Code to make it possible to write simple scrapers and leave the cleanup,
merging, and normalization of records to the harness that runs them."""
from __future__ import absolute_import

import logging
import sys
from datetime import datetime
from os import listdir
from os.path import dirname
from traceback import print_exc
from urlparse import urlparse

from .db import OBSOLETE_TABLES
from .db import TABLE_TO_KEY_FIELDS
from .db import create_table_if_not_exists
from .db import open_db
from .db import open_dt
from .db import show_tables
from .iso_8601 import iso_now
from .iso_8601 import from_iso
from .norm import TM_SYMBOLS
from .norm import clean_string
from .norm import merge
from .rating import DEFAULT_MIN_SCORE


log = logging.getLogger(__name__)

# keys that always match scraper_id
SCRAPER_ID_KEYS = {'campaign_id', 'scraper_id'}

# default place to look for scrapers
DEFAULT_SCRAPERS_PACKAGE = 'scrapers'


def run_scrapers(get_records, scraper_ids=None, skip_scraper_ids=None,
                 default_freq=None, scraper_to_freq=None,
                 scraper_to_last_changed=None, package=None):
    """Run scrapers.

    get_records -- takes a single argument (a scraper module) and yields
        tuples of (table, row) corresponding to the scraped data.
    scraper_ids -- whitelist of scrapers to run
    skip_scraper_ids -- blacklist of scrapers not to run (ignored if
        scraper_ids is set
    default_freq -- default frequency to run scrapers
    scraper_to_freq -- custom frequency to run particular scrapers
    scrapers_to_last_changed -- use to force scrapers to run. map from
        scraper_id to UTC datetime for when either the code or the data source
        last changed
    package -- package to find scraper modules in (default is 'scrapers')
    """
    failed = []

    for scraper_id in (scraper_ids or get_scraper_ids()):
        if not should_run_scraper(
                scraper_id, scraper_ids, skip_scraper_ids,
                default_freq, scraper_to_freq, scraper_to_last_changed):
            log.info('Skipping scraper: {}'.format(scraper_id))
            continue

        log.info('Launching scraper: {}'.format(scraper_id))
        try:
            scraper = load_scraper(scraper_id, package=package)
            records = get_records(scraper)
            save_records_from_scraper(records, scraper_id)
        except:
            failed.append(scraper_id)
            print_exc()

    # just calling exit(1) didn't register on morph.io
    if failed:
        raise Exception(
            'failed to scrape campaigns: {}'.format(', '.join(failed)))


def delete_records_from_scraper(scraper_id, db=None):
    """Clear all data from the given scraper."""
    if db is None:
        db = open_db()

    tables = show_tables(db)

    for table in tables:
        if table not in set(TABLE_TO_KEY_FIELDS) | set(OBSOLETE_TABLES):
            log.warn('Unknown table `{}`, not clearing'.format(table))
            continue

        db.rollback()
        db.execute(
            'DELETE FROM {} WHERE scraper_id = ?'.format(table),
            [scraper_id])
        db.commit()


def get_scraper_ids(package='scrapers'):
    __import__(package)
    package_dir = dirname(sys.modules[package].__file__)

    for filename in sorted(listdir(package_dir)):
        if filename.endswith('.py') and not filename.startswith('_'):
            yield filename[:-3]  # meow!


def load_scraper(scraper_id, package=None):
    package = package or DEFAULT_SCRAPERS_PACKAGE

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
                    _add('category', dict(
                        company=company, brand=brand, category=c))
            else:
                for category in record.pop('categories'):
                    _add('category', dict(
                        company=company, category=category))

        # assume min_score of 0 if not specified
        if 'score' in record and 'min_score' not in record:
            record['min_score'] = DEFAULT_MIN_SCORE

        # automatic brand entries
        if 'brand' in record and table != 'brand':
            _add('brand', dict(company=company, brand=brand))

        # automatic company entries
        if 'company' in record and table != 'company':
            _add('company', dict(company=company))

        # actually clean up and store/merge the record
        key_fields = [k for k in TABLE_TO_KEY_FIELDS[table]
                      if k not in SCRAPER_ID_KEYS]

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

        # catch empty keys
        for k in key_fields:
            if record.get(k) in (None, ''):
                if k == 'scope' or (k == 'brand' and table != 'brand'):
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


def save_records_from_scraper(records, scraper_id):
    table_to_key_to_row = {}

    for table, record in records:
        if table not in TABLE_TO_KEY_FIELDS:
            # campaign scrapers often don't specify "campaign_"
            if 'campaign_' + table in TABLE_TO_KEY_FIELDS:
                table = 'campaign_' + table
            else:
                raise ValueError('unknown table `{}`'.format(table))
        add_record(table, record, table_to_key_to_row)

    # add the time this campaign was scraped
    add_record('scraper',
               dict(last_scraped=iso_now()),
                    table_to_key_to_row)

    delete_records_from_scraper(scraper_id)

    dt = open_dt()

    for table in table_to_key_to_row:
        create_table_if_not_exists(table)

        key_fields = TABLE_TO_KEY_FIELDS[table]
        if 'scraper_id' not in key_fields:
            key_fields = ['scraper_id'] + key_fields
        scraper_id_keys = SCRAPER_ID_KEYS & set(key_fields)

        for key, row in table_to_key_to_row[table].iteritems():
            row = row.copy()
            for k in scraper_id_keys:
                row[k] = scraper_id

            dt.upsert(row, table)


def get_last_scraped(scraper_id, db=None):
    if db is None:
        db = open_db()

    sql = 'SELECT last_scraped FROM scraper where scraper_id = ?'

    rows = list(db.execute(sql, [scraper_id]))

    if rows:
        return from_iso(rows[0][0])
    else:
        return None


def should_run_scraper(
        scraper_id, scraper_ids, skip_scraper_ids,
        default_freq, scraper_to_freq, scraper_to_last_changed):

    # whitelist takes precedence
    if scraper_ids:
        return scraper_id in scraper_ids

    # then blacklist
    if skip_scraper_ids and scraper_id in skip_scraper_ids:
        return False

    # then look at frequency
    freq = (scraper_to_freq or {}).get(scraper_id, default_freq)
    if freq is None:
        return True

    last_scraped = get_last_scraped(scraper_id)
    if last_scraped is None:
        return True

    # doesn't matter if we scraped it before the last modification
    last_changed = (scraper_to_last_changed or {}).get(scraper_id)
    if last_changed and last_changed > last_scraped:
        return True

    now = datetime.utcnow()

    return (last_scraped + freq < now)
