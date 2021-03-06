import csv
import configparser
import logging
import psycopg2
from datetime import datetime
from weekly_food_sales_utilities import insert_table_batch

logging.basicConfig(
    filename='logs/weekly_food_sales_load_{}.log'.format(datetime.now().strftime("%Y-%m-%dT%H.%M.%S")),
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

config = configparser.ConfigParser()
config.read('config.ini')

pg_conn = None
pg_cursor = None


def parse_date(src_str):
    date_parts = src_str.split('/')
    return'{}-{}-{}'.format(date_parts[2], date_parts[0], date_parts[1])

try:
    logging.info('connecting to postgres')
    pg_config = config['postgres']
    pg_conn = psycopg2.connect(user=pg_config['user'],
                               password=pg_config['password'],
                               host=pg_config['host'],
                               port=pg_config['port'],
                               dbname=pg_config['db'])
    pg_cursor = pg_conn.cursor()

    f = open(config['src']['path'], 'r')

    pg_cursor.execute('truncate table national_total_and_subcategory')
    staged_records = []
    for src_record in csv.DictReader(f):
        staged_records.append(
            (
                parse_date(src_record['Date']),
                src_record['Category'],
                src_record['Subcategory'],
                src_record['Variable'],
                src_record['Value']
            )
        )
        # add records in batches of 1000
        if 1000 == len(staged_records):
            logging.info('inserting batch')
            insert_table_batch(pg_cursor, 'national_total_and_subcategory', staged_records)
            staged_records.clear()

    # add any remaining records
    if len(staged_records):
        logging.info('inserting last batch')
        insert_table_batch(pg_cursor, 'national_total_and_subcategory', staged_records)

    logging.info('committing changes to postgres')
    pg_conn.commit()

    # close the source file
    f.close()

except Exception as e:
    logging.exception(repr(e))
finally:
    logging.info('closing postgres connection')
    if pg_cursor is not None and not pg_cursor.closed:
        pg_cursor.close()
    if pg_conn is not None and not pg_conn.closed:
        pg_conn.close()
