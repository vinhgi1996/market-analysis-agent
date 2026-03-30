"""
api/__init__.py

This module automatically discovers and aggregates all FastAPI routers
defined in the `api/` package. It allows you to split your API endpoints
across multiple files while keeping a single unified router to include
in the main application.

Usage:
    from api import router as api_router
    app.include_router(api_router)
"""

import importlib
import pkgutil
from fastapi import APIRouter

# Create a top-level router that will aggregate all sub-routers
router = APIRouter()

# -------------------------------------------------------------------
# Dynamic Router Discovery
# -------------------------------------------------------------------
# This loop automatically imports all modules in the current package (`api/`)
# and registers any router defined in those modules.
# It eliminates the need to manually import and include each router file.
for _, module_name, _ in pkgutil.iter_modules(__path__):
    # Dynamically import the module by name (e.g., 'api.endpoints_v16')
    module = importlib.import_module(f"{__name__}.{module_name}")

    # Check if the module exposes a 'router' attribute
    # Each endpoint file must define a router like: router = APIRouter(...)
    if hasattr(module, "router"):
        # Include the module's router into the top-level aggregated router
        router.include_router(module.router)
