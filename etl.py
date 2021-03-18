import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

s3_buckets = [
                (
                    's3://udacity-dend/song_data',
                    'arn:aws:iam::962755301980:role/RedshiftCopyUnload'
                ), (
                    's3://udacity-dend/log_data',
                    'arn:aws:iam::962755301980:role/RedshiftCopyUnload',
                    's3://udacity-dend/log_json_path.json'
                )
]

def load_staging_tables(cur, conn, s3_buckets):
    for idx, query in enumerate(copy_table_queries):
        # print(f'Executing query: {query.format(*s3_buckets[idx])}')
        try:
            cur.execute(query.format(*s3_buckets[idx]))
            conn.commit()
        except Exception as err:
            print("Exception: " + err)
            conn.rollback()
            cur.execute('select * from stl_load_errors')
            errors = cur.fetchall()
            for error in errors:
                print(error)


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
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