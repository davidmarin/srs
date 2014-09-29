"""Code to make it possible to write simple scrapers and leave the cleanup,
merging, and normalization of records to the harness that runs them."""
import logging
import sys
from os import listdir
from os.path import dirname

import scraperwiki

from .db import TABLE_TO_KEY_FIELDS
from .db import TABLE_TO_EXTRA_FIELDS

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
