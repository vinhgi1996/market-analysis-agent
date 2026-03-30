"""
config/model_manager.py

This module manages AI model clients (Gemini and OpenAI) for the project.

Features:
- Centralized connection settings loaded from environment variables
- Singleton pattern per client type
- Lazy initialization (only create client when needed)
- Extendable for future model integrations (Anthropic, Claude, etc.)
"""

import os
import logging
from google import genai
from openai import OpenAI
from config.config import settings


logger = logging.getLogger(__name__)


class ModelManager:
    """
    A manager for handling AI model clients.

    Currently supports:
      - Gemini (Google GenAI)
      - OpenAI (GPT, o-series reasoning models, etc.)
    """

    # ----------------------------------------------------------------------
    # Gemini client configuration
    # ----------------------------------------------------------------------
    _gemini_client = None
    _gemini_api_key = settings.GEMINI_API_KEY

    # ----------------------------------------------------------------------
    # OpenAI client configuration
    # ----------------------------------------------------------------------
    _openai_client = None
    _openai_api_key = settings.OPENAI_API_KEY

    # ----------------------------------------------------------------------
    # Embedding model configuration
    # ----------------------------------------------------------------------
    _embedding_model = None
    _embedding_dim = None
    _embedding_model_name = "google/embeddinggemma-300m"

    # ----------------------------------------------------------------------
    # Gemini client accessor
    # ----------------------------------------------------------------------
    @classmethod
    def get_gemini_client(cls):
        """
        Get a singleton instance of the Gemini client.
        Initializes only once and reuses across the project.
        """
        if cls._gemini_client is None:
            if not cls._gemini_api_key:
                logger.error("Missing GEMINI_API_KEY environment variable.")
                raise ValueError("Missing GEMINI_API_KEY environment variable.")

            try:
                cls._gemini_client = genai.Client(api_key=cls._gemini_api_key)
                logger.info("Gemini client initialized successfully.")
            except Exception as e:
                logger.exception(f"Failed to initialize Gemini client: {e}")
                raise

        return cls._gemini_client

    # ----------------------------------------------------------------------
    # OpenAI client accessor
    # ----------------------------------------------------------------------
    @classmethod
    def get_openai_client(cls):
        """
        Get a singleton instance of the OpenAI client.

        Loads API key from environment variables or settings.
        This client can be used for:
          - GPT-4, GPT-4o models
          - o-series reasoning models (o1, o3, o4-mini, etc.)
          - Embeddings, Responses, etc.

        Raises:
            ValueError: If OPENAI_API_KEY is not set.
        """
        if cls._openai_client is None:
            if not cls._openai_api_key:
                logger.error("Missing OPENAI_API_KEY environment variable.")
                raise ValueError("Missing OPENAI_API_KEY environment variable.")

            try:
                cls._openai_client = OpenAI(api_key=cls._openai_api_key)
                logger.info("OpenAI client initialized successfully.")
            except Exception as e:
                logger.exception(f"Failed to initialize OpenAI client: {e}")
                raise

        return cls._openai_client


