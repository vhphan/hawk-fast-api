import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv

from databases.database import Database


class PgDB(Database):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self) -> None:
        if not hasattr(self, 'initialized'):  # Ensure __init__ is only called once
            self.driver = psycopg2
            super().__init__(self.driver)
            self.initialized = True

    def connect_to_database(self):
        return self.driver.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            database=os.getenv("PG_DATABASE"),
            # options=f"-c search_path={os.getenv('PG_SCHEMA')}"
        )

    def query(self, sql_statement, params=None):
        if params is None:
            params = ()
        self.cursor.execute(sql_statement, params)
        return self.cursor.fetchall(), [desc[0] for desc in self.cursor.description]

    def query_df(self, sql_statement, params=None):
        results, columns = self.query(sql_statement, params)
        return pd.DataFrame(results, columns=columns)

    def commit(self):
        self.connection.commit()

    def __enter__(self):
        self.connection = self.connect_to_database()
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.cursor.close()
        self.connection.close()


if __name__ == '__main__':
    load_dotenv()
    with PgDB() as db:
        sql = "SELECT now();"
        result, columns = db.query(sql)
        print(result)
        print(columns)
