import sqlite3
import os
import pandas as pd


DB_PATH = "../sql/candlethrob.db"

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def connect_db():
    """Creates a new SQLite connection."""
    return sqlite3.connect(DB_PATH, timeout=10)


def run_query(query, params=None):
    """Run a generic SQL query (SELECT, INSERT, etc.)."""
    with connect_db() as conn:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        conn.commit()
        return cursor


def fetch_all(query, params=None):
    """Run a SELECT query and return all results."""
    with connect_db() as conn:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor.fetchall()


def fetch_one(query, params=None):
    """Run a SELECT query and return the first row."""
    with connect_db() as conn:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor.fetchone()


def insert_dataframe(df, table_name):
    """Insert a DataFrame into a SQLite table."""
    try:
        with connect_db() as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"✅ Inserted {len(df)} rows into '{table_name}'.")
    except sqlite3.OperationalError as e:
        print("❌ SQLite error:", e)


def read_query_as_df(query, params=None):
    with connect_db() as conn:
        return pd.read_sql_query(query, conn, params=params)


def table_exists(table_name):
    """Check if a table exists in the database."""
    query = """
    SELECT name FROM sqlite_master 
    WHERE type='table' AND name=?;
    """
    result = fetch_one(query, (table_name,))
    return result is not None
