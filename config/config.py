"""
settings.py

Application-wide configuration using Pydantic v2 BaseSettings.
Loads environment variables from `.env.dev.<ENV>` files.

Usage:
    from settings import settings
    print(settings.PROJECT_NAME)
"""

from typing import List, Union
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
import os
import json
from pathlib import Path


# --------------------------------------------------
# Resolve ENV file path safely (independent of cwd)
# --------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_NAME = os.getenv("ENV", "dev")
ENV_FILE = BASE_DIR / f".env.{ENV_NAME}"


class Settings(BaseSettings):
    """
    Application configuration settings.
    """

    # -----------------------------
    # General
    # -----------------------------
    ENV: str = "dev"
    PROJECT_NAME: str = "Default Project"
    BACKEND_CORS_ORIGINS: Union[str, List[str]] = []

    # -----------------------------
    # AI / External APIs
    # -----------------------------
    OPENAI_API_KEY: str = ""

    GEMINI_API_KEY: str = ""
    GEMINI_MAX_TOKENS: int = 0

    # -----------------------------
    # PostgreSQL
    # -----------------------------
    POSTGRES_USER: str = ""
    POSTGRES_PASSWORD: str = ""
    POSTGRES_DB: str = ""
    POSTGRES_HOST: str = ""
    POSTGRES_PORT: str = ""

    # -----------------------------
    # Workers / Batch
    # -----------------------------
    MAX_WORKERS: int = 0
    PAGES_PER_BATCH: int = 0
    EMBED_BATCH_SIZE: int = 0
    QDRANT_BATCH_SIZE: int = 0

    # -----------------------------
    # MinIO / Storage
    # -----------------------------
    END_POINT: str = ""
    ACCESS_KEY: str = ""
    SECRET_KEY: str = ""
    MINIO_SECURE: bool = False

    # -----------------------------
    # Qdrant
    # -----------------------------
    QDRANT_LOCATION: str = ""
    QDRANT_PORT: int = 0

    # -----------------------------
    # Redis (Required)
    # -----------------------------
    REDIS_HOST: str = ""
    REDIS_PORT: int = 0

    # --------------------------------------------------
    # Field Validators
    # --------------------------------------------------

    @field_validator("BACKEND_CORS_ORIGINS", mode="before")
    @classmethod
    def parse_cors_origins(cls, v: Union[str, List[str]]) -> List[str]:
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return []

            # JSON format
            if v.startswith("[") and v.endswith("]"):
                try:
                    return json.loads(v)
                except json.JSONDecodeError:
                    raise ValueError("Invalid JSON for BACKEND_CORS_ORIGINS")

            # Comma separated
            return [origin.strip() for origin in v.split(",") if origin.strip()]

        elif isinstance(v, list):
            return v

        return []

    # --------------------------------------------------
    # Pydantic v2 Config
    # --------------------------------------------------
    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        case_sensitive=True,
        extra="ignore"
    )


# --------------------------------------------------
# Global instance
# --------------------------------------------------
settings = Settings()

if __name__ == "__main__":
    print("Loaded ENV file:", ENV_FILE)
    print("ENV exists:", ENV_FILE.exists())
    print("REDIS_HOST:", settings.REDIS_HOST)