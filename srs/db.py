"""Table definitions, opening and downloading sqlite databases."""
import logging
import sqlite3
from decimal import Decimal
from os import environ
from os.path import exists
from urllib import urlencode

import dumptruck
from dumptruck import DumpTruck

from .scrape import download

log = logging.getLogger(__name__)

DB_FILE_EXT = '.sqlite'

DEFAULT_DB_NAME = 'data'
DEFAULT_DB_PATH = DEFAULT_DB_NAME + DB_FILE_EXT

# map from table name to fields used for the primary key (not including
# campaign_id). All key fields are currently TEXT
TABLE_TO_KEY_FIELDS = {
    # factual information about a brand (e.g. company, url, etc.)
    'brand': ['company', 'brand'],
    # factual information about which categories a brand belongs to
    'brand_category': ['company', 'brand', 'category'],
    # info about a campaign's creator, etc.
    'campaign': ['campaign_id'],
    # map from brand in campaign to canonical version
    'campaign_brand_map': [
        'campaign_id', 'campaign_company', 'campaign_brand'],
    # should you buy this brand?
    'campaign_brand_rating': ['campaign_id', 'company', 'brand', 'scope'],
    # map from category in campaign to canonical version
    'campaign_category_map': ['campaign_id', 'campaign_category'],
    # map from company in campaign to canonical version
    'campaign_company_map': ['campaign_id', 'campaign_company'],
    # should you buy from this company?
    'campaign_company_rating': ['campaign_id', 'company', 'scope'],
    # category hierarchy information
    'category': ['category'],
    # factual information about a company (e.g. url, email, etc.)
    'company': ['company'],
    # factual information about which categories a company belongs to
    'company_category': ['company', 'category'],
    # used to track when a scraper last ran
    'scraper': [],
}

_RATING_FIELDS = [
    # -1 (bad), 0 (mixed), or 1 (good). Lingua franca of ratings
    ('judgment', 'TINYINT'),
    # letter grade
    ('grade', 'TEXT'),
    # written description (e.g. cannot recommend)
    ('description', 'TEXT'),
    # numeric score (higher numbers are good)
    ('score', 'NUMERIC'),
    ('min_score', 'NUMERIC'),
    ('max_score', 'NUMERIC'),
    # ranking (low numbers are good)
    ('rank', 'INTEGER'),
    ('num_ranked', 'INTEGER'),
    # url for details about the rating
    ('url', 'TEXT'),
]

TABLE_TO_EXTRA_FIELDS = {
    'campaign_brand_map': [('company', 'TEXT'), ('brand', 'TEXT')],
    'campaign_brand_rating': _RATING_FIELDS,
    'campaign_category_map': [('category', 'TEXT')],
    'campaign_company_map': [('company', 'TEXT')],
    'campaign_company_rating': _RATING_FIELDS,
    'scraper': [('last_scraped', 'TEXT')],
}


def download_db(db_name, morph_project='spendright-scrapers', force=False):
    """Download the given DB from morph.io. If force is False (the default)
    only download it if there isn't already a local file by that name."""
    db_path = db_name + DB_FILE_EXT
    if force or not exists(db_path):
        if 'MORPH_API_KEY' not in environ:
            raise ValueError(
                'Must set MORPH_API_KEY to download {} db'.format(db_name))

        url = 'https://morph.io/{}/{}/data.sqlite?{}'.format(
            morph_project, db_name, urlencode(
                {'key': environ['MORPH_API_KEY']}))

        log.info('downloading {} -> {}'.format(url, db_path))
        download(url, db_path)


def open_db(db_name=DEFAULT_DB_NAME):
    """Open the (local) sqlite database of the given name."""
    return sqlite3.connect(db_name + DB_FILE_EXT)


def open_dt(db_name=DEFAULT_DB_NAME):
    """Open a dumptruck for the sqlite database of the given name."""
    return DumpTruck(db_name + DB_FILE_EXT)


def create_table_if_not_exists(table,
                               db=None,
                               with_scraper_id=True):
    """Create the given table if it does not already exist in the given
    db.

    If with_scraper_id is True (default) include a scraper_id column
    in the primary key for each table.
    """
    if db is None:
        db = open_db()

    key_fields = TABLE_TO_KEY_FIELDS[table]
    if with_scraper_id:
        key_fields = ['scraper_id'] + key_fields

    sql = 'CREATE TABLE IF NOT EXISTS `{}` ('.format(table)
    for k in key_fields:
        sql += '`{}` TEXT, '.format(k)
    for k, field_type in TABLE_TO_EXTRA_FIELDS.get(table) or ():
        sql += '`{}` {}, '.format(k, field_type)
    sql += 'PRIMARY KEY ({}))'.format(', '.join(key_fields))

    db.execute(sql)


def use_decimal_type_in_sqlite():
    """Use Decimal type for reals in sqlite3. Not reversible."""
    dumptruck.PYTHON_SQLITE_TYPE_MAP.setdefault(Decimal, 'real')
    sqlite3.register_adapter(Decimal, str)


def show_tables(db):
    """List the tables in the given db."""
    sql = "SELECT name FROM sqlite_master WHERE type = 'table'"
    return sorted(row[0] for row in db.execute(sql))
