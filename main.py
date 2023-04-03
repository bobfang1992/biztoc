import feedparser
import sqlite3
from sqlite3 import Error
import json


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return conn


def create_table(conn):
    create_table_sql = """CREATE TABLE IF NOT EXISTS news_entries (
                            id TEXT PRIMARY KEY,
                            title TEXT,
                            link TEXT,
                            published TEXT,
                            json_data TEXT
                        );"""
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def insert_or_replace_entry(conn, entry):
    sql = '''INSERT OR REPLACE INTO news_entries(id, title, link, published, json_data) VALUES(?, ?, ?, ?, ?)'''
    cur = conn.cursor()
    cur.execute(sql, entry)
    return cur.lastrowid


def count_rows(conn):
    sql = '''SELECT COUNT(*) FROM news_entries'''
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchone()[0]


def main():
    database = r"news_entries.db"

    # create a database connection
    conn = create_connection(database)

    # create table
    create_table(conn)

    NewsFeed = feedparser.parse("https://biztoc.com/feed")
    entries = NewsFeed.entries
    print("total number of entries:", len(entries))

    # upsert entries into the SQLite database
    with conn:
        for entry in entries:
            data = (entry["id"], entry["title"], entry["link"],
                    entry["published"], json.dumps(entry))
            insert_or_replace_entry(conn, data)

        # print the number of rows in the table
        print("rows in the database:", count_rows(conn))


if __name__ == '__main__':
    main()
