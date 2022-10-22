import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries



def load_staging_tables(cur, conn):
    """
    - load song_data and log_data from s3 bucket
    """    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    - insert the data into all tables from insert_table_queries in sql_queries file
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - connect to amazon redshift cluster
    - execute load_staging_tables and insert_tables functions
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()