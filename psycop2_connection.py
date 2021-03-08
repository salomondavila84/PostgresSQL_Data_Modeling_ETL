'''
Reference:
- https://www.python.org/dev/peps/pep-0249/
- https://www.psycopg.org/docs/
'''
import psycopg2

# Creating connection to database
# ToDo: Set system variable for username and password
try:
    conn = psycopg2.connect(database='songs', user='db_student', password="setMyPassword",
                        host="127.0.0.1", port="5432", sslmode="disable", gssencmode="disable")

except psycopg2.Error as e:
    print(f"{e}: Could not make connection to the Postgres database")


conn.set_session(autocommit=True)

# Deleting table
cur = conn.cursor()

try:
    cur.execute("""
    DROP table music_library
    ;
    """)

except psycopg2.Error as e:
    print(f"{e}: Error: Dropping table")

#conn.commit()

# Creating table
try:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS music_library (album_name varchar, artist_name varchar, year int)
    ;
    """
    )
except psycopg2.Error as e:
    print(f"{e} :Error Issue creating table")

#conn.commit()

# Inserting to the table
try:
    cur.execute("""
    INSERT INTO music_library (album_name, artist_name, year) VALUES (%s, %s, %s);
    """, ("Let It Be", "The Beatles", 1970))
except psycopg2.Error as e:
    print(f"{e} :Error inserting Rows")

try:
    cur.execute("""
    INSERT INTO music_library (album_name, artist_name, year) VALUES (%s, %s, %s);
    """, ("Rubber Soul", "The Beatles", 1965))
except psycopg2.Error as e:
    print(f"{e} :Error inserting Rows")


cur.close()

# Initial query with a complete resulting set and iterating the printing
cur = conn.cursor()

cur.execute("""
SELECT * FROM music_library
;
""")

results = cur.fetchall()
for record in results:
    print(record[1])

#conn.commit()

# Launching another query for a single row at a time
cur = conn.cursor()

cur.execute("""
SELECT * FROM music_library
;
""")

# Fetch the next row of a query result set, returning a single sequence, or None when no more data is available.
row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

#conn.commit()


cur.close()
conn.close()
