import psycopg2
import config
from sql_queries import create_table_queries, drop_table_queries

def create_database():
    """
    Establish database connection and return the connection object and cursor reference.

    Returns:
        cur: psycopg2.extensions.cursor
            Database cursor reference
        conn: psycopg2.extensions.connection
            Database connection object
    """
    # connect to default database
    conn = psycopg2.connect(f"host=127.0.0.1 dbname={config.DB_NAME} user={config.DB_USERNAME} password={config.DB_PASSWORD}")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect(f"host=127.0.0.1 dbname=sparkifydb user={config.DB_USERNAME} password={config.DB_PASSWORD}")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drop all tables if exist. This is to reset all tables.

    Parameters:
        cur: psycopg2.extensions.cursor
            Database cursor reference
        conn: psycopg2.extensions.connection
            Database connection object
    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create all tables if not exist.

    Parameters:
        cur: psycopg2.extensions.cursor
            Database cursor reference
        conn: psycopg2.extensions.connection
            Database connection object
    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()