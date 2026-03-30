# config/redis_manager.py
"""
RedisManager
------------
Centralized manager for Redis connections.

Features:
- Async Redis client (singleton)
- Optional sync client (if you need it)
- Health check on startup
- Clean shutdown
- Env-driven configuration

Env vars:
  REDIS_HOST        (default: "localhost")
  REDIS_PORT        (default: 6379)
  REDIS_DB          (default: 0)
  REDIS_PASSWORD    (default: None)
  REDIS_TLS         (default: "false") # set "true" to enable TLS
  REDIS_HEALTHCHECK_INTERVAL (default: 30)
"""

from __future__ import annotations

import os
import logging
from typing import Optional

# Async client
from redis.asyncio import Redis as AsyncRedis
# Sync client (optional; only import if you actually use it)
from redis import Redis as SyncRedis

logger = logging.getLogger(__name__)


class RedisManager:
    # class-level singletons
    _async_client: Optional[AsyncRedis] = None
    _sync_client: Optional[SyncRedis] = None

    # --- Configuration (env-driven) ---
    _host: str = os.getenv("REDIS_HOST", "localhost")
    _port: int = int(os.getenv("REDIS_PORT", "6379"))
    #_db: int = int(os.getenv("REDIS_DB", "0"))
    #_password: Optional[str] = os.getenv("REDIS_PASSWORD") or None
    #_use_tls: bool = os.getenv("REDIS_TLS", "false").lower() in ("1", "true", "yes")
    #_health_check_interval: int = int(os.getenv("REDIS_HEALTHCHECK_INTERVAL", "30"))

    # --------- Async client lifecycle ---------
    @classmethod
    async def init_async_client(cls) -> None:
        """
        Initialize the async Redis client (idempotent).
        Performs a ping() health check.
        """
        if cls._async_client is not None:
            return

        logger.info(
            "Initializing async Redis client: host=%s port=%s",
            cls._host, cls._port,
            #cls._db, cls._use_tls,
        )

        cls._async_client = AsyncRedis(
            host=cls._host,
            port=cls._port,
            # db=cls._db,
            #password=cls._password,
            #decode_responses=True,           # return str instead of bytes
            #health_check_interval=cls._health_check_interval,
            #ssl=cls._use_tls or None,        # set True to enable TLS
        )

        # Health check early to fail fast
        await cls._async_client.ping()
        logger.info("Async Redis client is ready.")

    @classmethod
    def get_async_client(cls) -> AsyncRedis:
        """
        Get the initialized async Redis client.
        Raises if not initialized (enforced by lifespan).
        """
        if cls._async_client is None:
            raise RuntimeError("Async Redis client not initialized. Call init_async_client() in app lifespan.")
        return cls._async_client

    @classmethod
    async def close_async_client(cls) -> None:
        """
        Close the async Redis client if present.
        """
        if cls._async_client is not None:
            try:
                await cls._async_client.aclose()
            finally:
                cls._async_client = None
                logger.info("Async Redis client closed.")

    # --------- (Optional) Sync client lifecycle ---------
    @classmethod
    def init_sync_client(cls) -> None:
        """
        Initialize the sync Redis client (idempotent).
        Performs a ping() health check.
        """
        if cls._sync_client is not None:
            return

        logger.info(
            "Initializing sync Redis client: host=%s port=%s",
            cls._host, cls._port
            #, cls._db, cls._use_tls,
        )

        cls._sync_client = SyncRedis(
            host=cls._host,
            port=cls._port,
            # db=cls._db,
            # password=cls._password,
            # decode_responses=True,
            # health_check_interval=cls._health_check_interval,
            # ssl=cls._use_tls or None,
        )
        # Health check early to fail fast
        cls._sync_client.ping()
        logger.info("Sync Redis client is ready.")

    @classmethod
    def get_sync_client(cls) -> SyncRedis:
        """
        Get the initialized sync Redis client.
        Raises if not initialized (enforced by lifespan).
        """
        if cls._sync_client is None:
            raise RuntimeError("Sync Redis client not initialized. Call init_sync_client() in app lifespan.")
        return cls._sync_client

    @classmethod
    def close_sync_client(cls) -> None:
        """
        Close the sync Redis client if present.
        """
        if cls._sync_client is not None:
            try:
                cls._sync_client.close()
            finally:
                cls._sync_client = None
                logger.info("Sync Redis client closed.")
