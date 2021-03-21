import configparser
import psycopg2
import argparse

from sql_queries import copy_table_queries, insert_table_queries

s3_buckets = [
                [
                    's3://udacity-dend/song_data',
                    'arn:aws:iam::962755301980:role/RedshiftCopyUnload'
                ], [
                    's3://udacity-dend/log_data',
                    'arn:aws:iam::962755301980:role/RedshiftCopyUnload',
                    's3://udacity-dend/log_json_path.json'
                ]
]

def load_staging_tables(cur, conn, s3_buckets):
    for idx, query in enumerate(copy_table_queries):
        print(f'Executing query: {query.format(*s3_buckets[idx])}')
        try:
            cur.execute(query.format(*s3_buckets[idx]))
            conn.commit()
        except Exception as err:
            print_copy_diagnostics(cur, conn, err)
            raise


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(f'Executing query: {query}')
        try:
            cur.execute(query)
            conn.commit()
        except Exception as err:
            print("Insert exception: " + str(err))
            raise


def print_copy_diagnostics(cur, conn, err=None):
    """Print diagnostic information stored in stl_load_errors system table
    """

    if err is not None:
        print("Load exception: " + str(err))
    conn.commit()
    cur.execute("select to_char(starttime, 'DD-MM HH:MI:SS'), trim(colname), position, raw_field_value, err_reason, trim(filename) " +
                "from stl_load_errors;")
    errors = cur.fetchall()
    for error in errors:
        print(f'Load error: {error}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action="store_true")
    args = parser.parse_args()
    if args.test:
        s3_buckets[0][0] = 's3://udacity-dend/song-data/A/A/'
        s3_buckets[1][0] = 's3://udacity-dend/log-data/2018/11/2018-11-01-events.json'
        print('Loading test data subset.')

    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    arn = config['IAM_ROLE']['ARN']

    print("Connection string: host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn, s3_buckets)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()