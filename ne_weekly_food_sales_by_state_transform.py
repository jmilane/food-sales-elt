import csv
import logging
from datetime import datetime
from dateutil import tz


def ne_weekly_food_sales_by_state_transform(config, pg_cursor):
    logging.info('***** started ne_weekly_food_sales_by_state_transform() *****')
    pg_cursor.execute('''select
                         date,
                         substring(state from 1 for (length(state) - length(' - Multi Outlet + Conv'))),
                         sum(value)
                         from state_and_category
                         where variable = 'Dollars'
                         and
                         (
                           state = 'Connecticut - Multi Outlet + Conv'
                           or state = 'Maine - Multi Outlet + Conv'
                           or state = 'Massachusetts - Multi Outlet + Conv'
                           or state = 'New Hampshire - Multi Outlet + Conv'
                           or state = 'Rhode Island - Multi Outlet + Conv'
                           or state = 'Vermont - Multi Outlet + Conv'
                         )
                         group by
                         date,
                         state
                         order by
                         date,
                         state''')
    results = pg_cursor.fetchall()

    staged_data = dict()
    for result in results:
        dt_date = datetime.strptime(str(result[0]), '%Y-%m-%d')
        dt_date = dt_date.replace(tzinfo=tz.gettz('Pacific/Honolulu'))
        state = result[1]
        dollars = round(result[2])
        epoch = int(dt_date.timestamp() * 1000)
        if epoch not in staged_data:
            staged_data[epoch] = dict()
        staged_data[epoch][state] = dollars

    states = list()
    for state in sorted(next(iter(staged_data.values()))):
        states.append(state)

    logging.info('writing to file')
    dest_file_writer = open(config['ne_weekly_food_sales_by_state']['dest_path'], 'w')
    dest_file_csv_writer = csv.writer(dest_file_writer)
    header_row = ['Epoch'] + states
    dest_file_csv_writer.writerow(header_row)
    for epoch in sorted(staged_data.keys()):
        data_row = list()
        data_row.append(epoch)
        for state in states:
            data_row.append(staged_data[epoch][state])
        dest_file_csv_writer.writerow(data_row)
    dest_file_writer.close()
