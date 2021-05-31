import csv
import logging
from weekly_food_sales_utilities import insert_table_batch
from weekly_food_sales_utilities import parse_date


def ne_weekly_food_sales_by_state_load(config, pg_conn, pg_cursor):
    logging.info('***** started us_weekly_food_sales_by_category_load() *****')

    pg_cursor.execute('truncate table state_and_category')
    staged_records = []
    f = open(config['ne_weekly_food_sales_by_state']['src_path'], 'r')
    for src_record in csv.DictReader(f):
        staged_records.append(
            (
                parse_date(src_record['Date']),
                src_record['State'],
                src_record['Category'],
                src_record['variable'],
                None if 'NA' == src_record['value'] else src_record['value']
            )
        )
        # add records in batches of 1000
        if 1000 == len(staged_records):
            logging.info('inserting batch')
            insert_table_batch(pg_cursor, 'state_and_category', staged_records)
            staged_records.clear()

    # add any remaining records
    if len(staged_records):
        logging.info('inserting last batch')
        insert_table_batch(pg_cursor, 'state_and_category', staged_records)

    logging.info('committing changes to postgres')
    pg_conn.commit()

    # close the source file
    f.close()
