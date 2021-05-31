import csv
import logging
from datetime import datetime
from dateutil import tz


def ne_weekly_food_sales_by_state_stacked_transform(config, pg_cursor):
    logging.info('***** started ne_weekly_food_sales_by_state_stacked_transform() *****')
    pg_cursor.execute('''select
                         date,
                         state,
                         sum(value)
                         from state_and_category
                         where variable = 'Dollars'
                         and state in ('Connecticut', 'Maine', 'Massachusetts', 'New Hampshire', 'Rhode Island', 'Vermont')
                         group by
                         date,
                         state
                         order by
                         date,
                         state''')
    results = pg_cursor.fetchall()

    staged_data = dict()
    for result in results:
        date = result[0]
        state = result[1]
        dollars = round(result[2])
        if date not in staged_data:
            staged_data[date] = dict()
        staged_data[date][state] = dollars

    states = list()
    for state in sorted(next(iter(staged_data.values()))):
        states.append(state)

    logging.info('writing to file')
    dest_file_writer = open(config['ne_weekly_food_sales_by_state_stacked']['dest_path'], 'w')
    dest_file_csv_writer = csv.writer(dest_file_writer)
    header_row = ['Date'] + states
    dest_file_csv_writer.writerow(header_row)
    for date in sorted(staged_data.keys()):
        data_row = list()
        data_row.append(datetime.strftime(date, '%m-%d-%Y'))
        for state in states:
            data_row.append(staged_data[date][state])
        dest_file_csv_writer.writerow(data_row)
    dest_file_writer.close()
