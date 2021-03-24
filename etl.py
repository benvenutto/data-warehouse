import configparser
import psycopg2
import argparse

from sql_queries import copy_table_queries, insert_table_queries, update_table_queries

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


def load_staging_tables(cur, conn, bucket_config):
    """Copy data in S3 buckets into stagig tables.

    Args:
        cur:            an open cursor.
        conn:           a database connection.
        bucket_config:  copy command parameters for each S3 bucket.

    """
    for idx, query in enumerate(copy_table_queries):
        print(f'Executing copy query: {query.format(*bucket_config[idx])}')
        try:
            cur.execute(query.format(*bucket_config[idx]))
            conn.commit()
        except Exception as err:
            print_copy_diagnostics(cur, conn, err)
            raise
        print('Completed insert query.')


def insert_tables(cur, conn):
    """Insert data from staging tables into fact and dimension tables.

    Args:
        cur:            an open cursor.
        conn:           a database connection.

    """
    for query in insert_table_queries:
        print(f'Executing insert query: {query}')
        try:
            cur.execute(query)
            conn.commit()
        except Exception as err:
            print("Insert exception: " + str(err))
            raise
        print('Completed insert query.')


def update_tables(cur, conn):
    """Update previously inserted data in fact and dimension tables.

    Args:
        cur:            an open cursor.
        conn:           a database connection.

    """
    for query in update_table_queries:
        print(f'Executing update query: {query}')
        try:
            cur.execute(query)
            conn.commit()
        except Exception as err:
            print("Update exception: " + str(err))
            raise
        print('Completed update query.')


def print_copy_diagnostics(cur, conn, err=None):
    """Print copy diagnostic information stored in stl_load_errors system table

    Args:
        cur:            an open cursor.
        conn:           a database connection.

    """

    if err is not None:
        print("Load exception: " + str(err))
    conn.commit()   # commit to retain data inserted into catalog
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
        s3_buckets[0][0] = 's3://udacity-dend/song-data/A/A/A/'
        s3_buckets[1][0] = 's3://udacity-dend/log-data/2018/11/2018-11-05-events.json'
        print('Loading test data subset.')

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Connection string: host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn = psycopg2.connect(host=config['CLUSTER']['HOST'],
                            port=config['CLUSTER']['DB_PORT'],
                            database=config['CLUSTER']['DB_NAME'],
                            user=config['CLUSTER']['DB_USER'],
                            password=config['CLUSTER']['DB_PASSWORD'],
                            keepalives=1, keepalives_idle=30, keepalives_interval=12, keepalives_count=5
                            )
    cur = conn.cursor()

    load_staging_tables(cur, conn, s3_buckets)
    insert_tables(cur, conn)
    update_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()