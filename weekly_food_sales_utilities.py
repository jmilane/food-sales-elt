from psycopg2.extras import execute_values


def insert_table_batch(pg_cursor, table_name, data):
    if len(data):
        insert_file = open('sql/insert_{}.sql'.format(table_name), 'r')
        sql = insert_file.read()
        execute_values(pg_cursor,
                       sql,
                       data)
        insert_file.close()


def parse_date(src_str):
    date_parts = src_str.split('-')
    return'{}-{}-{}'.format(date_parts[0], date_parts[1], date_parts[2])
