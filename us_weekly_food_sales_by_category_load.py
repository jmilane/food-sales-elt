import csv
import logging
from weekly_food_sales_utilities import insert_table_batch


def us_weekly_food_sales_by_category_load(config, pg_conn, pg_cursor):
    logging.info('***** started us_weekly_food_sales_by_category_load() *****')

    pg_cursor.execute('truncate table national_total_and_subcategory')
    staged_records = []
    f = open(config['us_weekly_food_sales_by_category']['src_path'], 'r')
    for src_record in csv.DictReader(f):
        staged_records.append(
            (
                src_record['Date'],
                src_record['Category'],
                src_record['Subcategory'],
                src_record['Variable'],
                None if 'NA' == src_record['Value'] else src_record['Value']
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
