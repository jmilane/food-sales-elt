import configparser
import logging
import psycopg2
from datetime import datetime
from us_weekly_food_sales_by_category_load import us_weekly_food_sales_by_category_load
from us_weekly_food_sales_by_category_transform import us_weekly_food_sales_by_category_transform


logging.basicConfig(
    filename='logs/us_weekly_food_sales_by_category_load_transform_{}.log'.format(datetime.now().strftime("%Y-%m-%dT%H.%M.%S")),
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

config = configparser.ConfigParser()
config.read('config.ini')

pg_conn = None
pg_cursor = None
try:
    logging.info('connecting to postgres')
    pg_config = config['postgres']
    pg_conn = psycopg2.connect(user=pg_config['user'],
                               password=pg_config['password'],
                               host=pg_config['host'],
                               port=pg_config['port'],
                               dbname=pg_config['db'])
    pg_cursor = pg_conn.cursor()
    us_weekly_food_sales_by_category_load(config, pg_conn, pg_cursor)
    us_weekly_food_sales_by_category_transform(config, pg_cursor)

except Exception as e:
    logging.exception(repr(e))

finally:
    logging.info('closing postgres connection')
    if pg_cursor is not None and not pg_cursor.closed:
        pg_cursor.close()
    if pg_conn is not None and not pg_conn.closed:
        pg_conn.close()
