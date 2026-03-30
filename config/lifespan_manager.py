# managers/app_lifespan_manager.py
from contextlib import asynccontextmanager
from fastapi import FastAPI
from config.postgre_manager import PostgresManager
from config.redis_manager import RedisManager


class AppLifespanManager:
    """
    Combines lifespans of all services for FastAPI.

    Responsibilities:
    - Initializes async PostgreSQL pool
    - Ensures proper shutdown of all async resources
    - Keeps sync resources accessible globally
    """

    @classmethod
    @asynccontextmanager
    async def lifespan(cls, app: FastAPI):
        """
        FastAPI lifespan context manager.

        Startup sequence:
        1. Initialize sync PostgreSQL pool (always available)
        2. Initialize async PostgreSQL pool

        Shutdown sequence:

        2. Close async PostgreSQL pool
        3. Close sync PostgreSQL pool
        """
        # --- Startup ---
        # 1. Sync PostgreSQL pool
        PostgresManager.init_sync_pool()

        # 2. Async PostgreSQL pool
        await PostgresManager.init_async_pool()

        # Redis (async)
        await RedisManager.init_async_client()
        # If you also need sync Redis anywhere:
        # RedisManager.init_sync_client()

        print("All services initialized and ready.")

        # Yield control to FastAPI (app is now running)
        yield

        # --- Shutdown ---
        await PostgresManager.close_async_pool()
        PostgresManager.close_sync_pool()
        print("All services closed.")
