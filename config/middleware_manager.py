"""
middleware_manager.py

This module defines a MiddlewareManager class to handle the registration of
middlewares for the FastAPI application in a clean, reusable, and testable way.

Features:
- Encapsulates middleware registration logic inside a class.
- Currently configures:
  - CORS (Cross-Origin Resource Sharing) via FastAPI’s CORSMiddleware.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from config.config import settings
from typing import cast
from starlette.types import ASGIApp


class MiddlewareManager:
    """
    MiddlewareManager handles the global middlewares for the FastAPI app.

    This class provides a clean and reusable interface to register middlewares
    that apply to every request/response in the app lifecycle.

    Currently Configures:
    - CORS Middleware:
        Enables cross-origin requests from specified origins.
        Useful when the frontend and backend are hosted on different domains.

    Example:
        app = FastAPI()
        MiddlewareManager.register_middlewares(app)
    """

    @classmethod
    def register_middlewares(cls, app: FastAPI) -> None:
        """
        Register all middlewares for the FastAPI app.

        Args:
            app (FastAPI): The FastAPI application instance.

        Adds:
            - CORS Middleware:
                - allow_origins: From `settings.BACKEND_CORS_ORIGINS`
                - allow_credentials: True (cookies/auth headers allowed)
                - allow_methods: ["*"] (all HTTP methods allowed)
                - allow_headers: ["*"] (all headers allowed)
        """
        app.add_middleware(
            cast(type[ASGIApp], CORSMiddleware),  # Cast ensures type-checking compatibility
            allow_origins=settings.BACKEND_CORS_ORIGINS,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
