import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    conn = psycopg2.connect(host="127.0.0.1",port="5432", dbname="cta", user="cta_admin", \
                            password="chicago", sslmode="disable", gssencmode="disable")

    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # ToDo: Comment after first run
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb")


    #close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect(database='sparkifydb', user='cta_admin', password="chicago",
                            host="127.0.0.1", port="5432", sslmode="disable", gssencmode="disable")
    cur = conn.cursor()
    conn.set_session(autocommit=True)

    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()

    # ToDo: comment out the drop_tables if first time running on db
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()