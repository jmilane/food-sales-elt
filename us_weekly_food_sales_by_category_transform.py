import csv
import logging
from datetime import datetime
from dateutil import tz


def us_weekly_food_sales_by_category_transform(config, pg_cursor):
    logging.info('***** started us_weekly_food_sales_by_category_transform() *****')
    pg_cursor.execute('''select
                         date,
                         category,
                         sum(value)
                         from national_total_and_subcategory
                         where upper(category) <> 'ALL FOODS'
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
    dest_file_writer = open(config['us_weekly_food_sales_by_category']['dest_path'], 'w')
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
