import logging
from psycopg2.extras import RealDictCursor
from config.postgre_manager import PostgresManager  # Your connection manager

logger = logging.getLogger(__name__)


class PostgresSQLUtil:
    """
    Utility class for executing SQL queries against PostgreSQL.

    Provides both synchronous (psycopg2) and asynchronous (asyncpg) query helpers:
    - run_sql: Execute queries synchronously.
    - async_run_sql: Execute queries asynchronously and return rows.
    - async_execute: Execute queries asynchronously without returning rows.
    """

    # ---------------------------
    # Synchronous query execution
    # ---------------------------
    @classmethod
    def run_sql(cls, sql: str, params: tuple = None):
        """
        Execute a SQL query synchronously using PostgresManager's sync pool.

        Args:
            sql (str): SQL query string.
            params (tuple, optional): Query parameters.

        Returns:
            List[dict]: Rows as dictionaries (empty for non-SELECT or errors).
        """
        try:
            with PostgresManager.get_sync_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(sql, params)

                    if cursor.description:  # SELECT query
                        rows = cursor.fetchall()
                        return [dict(row) for row in rows]
                    else:  # INSERT / UPDATE / DELETE
                        conn.commit()
                        return []
        except Exception as e:
            logger.error(f"[PostgreSQL][SYNC] SQL execution failed: {e} | SQL={sql} | Params={params}")
            return []

    # ---------------------------
    # Asynchronous query execution
    # ---------------------------
    @classmethod
    async def async_run_sql(cls, sql: str, params: tuple = None):
        """
        Execute a SQL query asynchronously using PostgresManager's async pool.

        Args:
            sql (str): SQL query string.
            params (tuple, optional): Query parameters.

        Returns:
            List[dict]: Rows as dictionaries (empty for non-SELECT or errors).
        """
        try:
            async with PostgresManager.get_async_connection() as conn:
                async with conn.transaction():
                    result = await conn.fetch(sql, *params if params else [])

                    if result:
                        return [dict(r) for r in result]
                    else:
                        return []
        except Exception as e:
            logger.error(f"[PostgreSQL][ASYNC] SQL execution failed: {e} | SQL={sql} | Params={params}")
            return []

    @classmethod
    async def async_execute(cls, sql: str, params: tuple = None) -> bool:
        """
        Execute an async SQL command that does not return rows
        (INSERT, UPDATE, DELETE, etc.).

        Args:
            sql (str): SQL command string.
            params (tuple, optional): Query parameters.

        Returns:
            bool: True if execution succeeded, False otherwise.
        """
        try:
            async with PostgresManager.get_async_connection() as conn:
                async with conn.transaction():
                    await conn.execute(sql, *params if params else [])
                    return True
        except Exception as e:
            logger.error(f"[PostgreSQL][ASYNC-EXECUTE] SQL execution failed: {e} | SQL={sql} | Params={params}")
            return False
