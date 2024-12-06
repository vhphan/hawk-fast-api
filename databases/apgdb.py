import asyncio
import csv
import os

from async_lru import alru_cache
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from databases.database import Database
from utils.tasks import clock


class PgDB(Database):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self) -> None:
        self.engine = None
        self.SessionLocal = None
        if not hasattr(self, 'initialized'):  # Ensure __init__ is only called once
            super().__init__(None)
            self.initialized = True

    async def connect_to_database(self):
        self.engine = create_async_engine(
            f"postgresql+asyncpg://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}",
            echo=True,
        )
        self.SessionLocal = async_sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine,
            class_=AsyncSession
        )

    @clock
    async def query(self, sql_statement, return_records=True, params=None):
        async with self.SessionLocal() as session:
            if params is None:
                params = {}
            result = await session.execute(text(sql_statement), params)
            rows = result.fetchall() if result.returns_rows else []
            columns = result.keys() if result.returns_rows else []
        if return_records:
            rows_ = [dict(zip(columns, row)) for row in rows]
            return rows_
        return rows, columns

    async def execute(self, sql_statement, params=None):
        async with self.SessionLocal() as session:
            if params is None:
                params = {}
            await session.execute(text(sql_statement), params)
            await session.commit()

    async def close_connection(self):
        await self.engine.dispose()

    async def __aenter__(self):
        await self.connect_to_database()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close_connection()


async def get_db():
    async with PgDB() as db:
        yield db


if __name__ == '__main__':
    load_dotenv()


    async def main():
        async with PgDB() as db:
            sql = text('select now() as curr_time;')  # Example query
            result, columns = await db.query(sql, return_records=False)
            print(result)
            print(list(columns))


    asyncio.run(main())
