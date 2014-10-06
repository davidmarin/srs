"""Table definitions etc."""
import dumptruck
import sqlite3
from decimal import Decimal

import scraperwiki


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



def create_table_if_not_exists(table,
                               with_scraper_id=True,
                               execute=None):
    """Create the given table if it does not already exist.

    If with_scraper_id is True (default) include a scraper_id column
    in the primary key for each table.

    Generated SQL will be passed to execute (default
    is scraperwiki.sql.execute())
    """
    if execute is None:
        execute = scraperwiki.sql.execute

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


def use_decimal_type_in_sqlite():
    """Use Decimal type for reals in sqlite3. Not reversible."""
    dumptruck.PYTHON_SQLITE_TYPE_MAP.setdefault(Decimal, 'real')
    sqlite3.register_adapter(Decimal, str)


def show_tables(execute=None):
    """List the tables in the given db."""
    sql = "SELECT name FROM sqlite_master WHERE type = 'table'"

    if execute is None:
        rows = scraperwiki.sql.execute(sql)['data']
    else:
        rows = list(execute(sql))

    return sorted(row[0] for row in rows)
