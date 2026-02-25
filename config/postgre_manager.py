# managers/postgres_manager.py
from typing import Optional, Generator, AsyncGenerator
from contextlib import contextmanager, asynccontextmanager
from psycopg2 import pool
from psycopg2.extensions import connection as Psycopg2Connection
import asyncpg
from config.config import settings


class PostgresManager:
    """
    Manages PostgreSQL connections (sync + async) in a modular way.

    Features:
    - Thread-safe synchronous pool (psycopg2)
    - Asynchronous pool (asyncpg)
    - Context managers for acquiring/releasing connections
    - Startup/shutdown lifecycle methods for FastAPI integration
    """

    # Class-level pools
    _sync_pool: Optional[pool.ThreadedConnectionPool] = None
    _async_pool: Optional[asyncpg.pool.Pool] = None

    # -----------------------------
    # Synchronous PostgreSQL
    # -----------------------------
    @classmethod
    def init_sync_pool(cls, minconn: int = 1, maxconn: int = 10):
        """
        Initialize the synchronous PostgreSQL pool if not already initialized.
        This pool is thread-safe and can be shared globally across the app.
        """
        if cls._sync_pool is None:
            cls._sync_pool = pool.ThreadedConnectionPool(
                minconn=minconn,
                maxconn=maxconn,
                dbname=settings.POSTGRES_DB,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
            )

    @classmethod
    def close_sync_pool(cls):
        """
        Close all connections in the synchronous pool.
        Should be called during app shutdown to free resources.
        """
        if cls._sync_pool:
            cls._sync_pool.closeall()
            cls._sync_pool = None

    @classmethod
    @contextmanager
    def get_sync_connection(cls) -> Generator[Psycopg2Connection, None, None]:
        """
        Context manager for synchronous PostgreSQL connections.
        - Acquires a connection from the pool on entry.
        - Releases the connection back to the pool on exit.
        """
        if cls._sync_pool is None:
            cls.init_sync_pool()
        conn = cls._sync_pool.getconn()
        try:
            yield conn
        finally:
            cls._sync_pool.putconn(conn)

    # -----------------------------
    # Asynchronous PostgreSQL
    # -----------------------------
    @classmethod
    async def init_async_pool(cls, min_size: int = 1, max_size: int = 10):
        """
        Initialize the asynchronous PostgreSQL pool if not already initialized.
        - asyncpg pool supports high concurrency and async usage.
        """
        if cls._async_pool is None:
            cls._async_pool = await asyncpg.create_pool(
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                database=settings.POSTGRES_DB,
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                min_size=min_size,
                max_size=max_size,
            )

    @classmethod
    async def close_async_pool(cls):
        """
        Close all connections in the async pool.
        Should be called during app shutdown to prevent leaks.
        """
        if cls._async_pool:
            await cls._async_pool.close()
            cls._async_pool = None

    @classmethod
    @asynccontextmanager
    async def get_async_connection(cls) -> AsyncGenerator[asyncpg.Connection, None]:
        """
        Context manager for asynchronous PostgreSQL connections.
        - Acquires a connection from the async pool on entry.
        - Releases the connection back to the pool on exit.
        """
        if cls._async_pool is None:
            await cls.init_async_pool()
        async with cls._async_pool.acquire() as conn:
            yield conn
