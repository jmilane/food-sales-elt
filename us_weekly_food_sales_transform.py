import configparser
import csv
import logging
import psycopg2
from datetime import datetime
from dateutil import tz


logging.basicConfig(
    filename='logs/us_weekly_food_sales_transform_{}.log'.format(datetime.now().strftime("%Y-%m-%dT%H.%M.%S")),
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

    # store list ids and names in a dictionary for lookup
    pg_cursor.execute('''select
                         date,
                         category,
                         sum(value)
                         from national_total_and_subcategory
                         where category <> 'All Foods'
                         and variable = 'Dollars'
                         group by
                         date,
                         category
                         order by
                         date,
                         category''')
    results = pg_cursor.fetchall()

    staged_data = dict()
    for result in results:
        dt_date = datetime.strptime(str(result[0]), '%Y-%m-%d')
        dt_date = dt_date.replace(tzinfo=tz.gettz('Pacific/Honolulu'))
        category = result[1]
        dollars = round(result[2])
        epoch = int(dt_date.timestamp() * 1000)
        if epoch not in staged_data:
            staged_data[epoch] = dict()
        staged_data[epoch][category] = dollars

    categories = list()
    for category in sorted(next(iter(staged_data.values()))):
        categories.append(category)

    logging.info('writing to file')
    dest_file_writer = open(config['dest']['path'], 'w')
    dest_file_csv_writer = csv.writer(dest_file_writer)
    header_row = ['Epoch'] + categories
    dest_file_csv_writer.writerow(header_row)
    for epoch in sorted(staged_data.keys()):
        data_row = list()
        data_row.append(epoch)
        for category in categories:
            data_row.append(staged_data[epoch][category])
        dest_file_csv_writer.writerow(data_row)
    dest_file_writer.close()


except Exception as e:
    logging.exception(repr(e))
finally:
    logging.info('closing postgres connection')
    if pg_cursor is not None and not pg_cursor.closed:
        pg_cursor.close()
    if pg_conn is not None and not pg_conn.closed:
        pg_conn.close()
