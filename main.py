"""
main.py

This is the main entry point for the FastAPI application. It performs the following:
- Sets a fallback environment variable for local development/debugging
- Creates the FastAPI app instance
- Registers middlewares (CORS, etc.)
- Includes all API routers
- Registers startup and shutdown hooks to initialize and close PostgreSQL pools
- Starts the app with Uvicorn if executed as a script
"""

import os
import uvicorn
from fastapi import FastAPI

from config.lifespan_manager import AppLifespanManager
from config.middleware_manager import MiddlewareManager  # Middleware registration (CORS, etc.)
from config.config import settings  # Centralized project settings
from api import router as api_router  # API endpoints router

# ---------------------------------------------------------------------
# Environment fallback for debugging in PyCharm or local dev
# ---------------------------------------------------------------------
# Ensures that the 'ENV' variable has a default value if not set
# This determines which .env file will be loaded by Settings
os.environ.setdefault("ENV", "dev")


# ---------------------------------------------------------------------
# Create FastAPI app
# ---------------------------------------------------------------------
def create_app() -> FastAPI:
    """
    Factory function to create and configure a FastAPI application.

    Steps:
    1. Create FastAPI instance with a project title from settings
    2. Register middlewares (CORS, etc.)
    3. Include API routers for all endpoints
    4. Register startup/shutdown hooks to initialize/close PostgreSQL pools
    """
    application = FastAPI(title=settings.PROJECT_NAME, lifespan=AppLifespanManager.lifespan)

    # -----------------------------
    # Middlewares
    # -----------------------------
    # Add global middlewares such as CORS using centralized settings
    MiddlewareManager.register_middlewares(application)

    # -----------------------------
    # API Routers
    # -----------------------------
    # Include all endpoint routers under a single main app
    # This allows modular endpoints in separate files
    application.include_router(api_router)


    return application


# ---------------------------------------------------------------------
# Create app instance
# ---------------------------------------------------------------------
# This instance is imported by Uvicorn or used in testing
app = create_app()

# ---------------------------------------------------------------------
# Run the application via Uvicorn when executing main.py directly
# ---------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(
        "main:app",  # Module-level reference to FastAPI app
        host="127.0.0.1",
        port=8000,
        reload=False  # Set True for auto-reload in dev
    )
