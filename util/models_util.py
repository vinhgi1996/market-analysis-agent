"""
utils/models_util.py

Utility functions for interacting with model clients (e.g., Gemini).
Provides async wrappers for content generation and embeddings.
"""

import asyncio
import logging
from config.model_manager import ModelManager
from functools import partial

logger = logging.getLogger(__name__)


class ModelUtil:
    """
    Utility layer for model-related operations.
    Uses ModelManager under the hood to fetch the correct client.
    """
    @classmethod
    async def _gemini_generate_answer_async(cls, prompt: str, model: str = "gemini-3-flash-preview"):
        """Internal async version (runs in executor)."""
        client = ModelManager.get_gemini_client()
        loop = asyncio.get_running_loop()

        fn = partial(client.models.generate_content, model=model, contents=prompt)
        return await loop.run_in_executor(None, fn)

    @classmethod
    def gemini_generate_answer(cls, prompt: str, model: str = "gemini-3-flash-preview"):
        """
        Generate an answer from the Gemini model.
        Works in both sync and async contexts.

        Args:
            prompt (str): The input prompt.
            model (str, optional): Model name to use. Defaults to "gemini-2.5-flash".

        Returns:
            The response object from Gemini API.
        """
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                # Inside an async context
                return cls._gemini_generate_answer_async(prompt, model)
        except RuntimeError:
            # No event loop -> sync context
            pass

        # Fallback: run sync (blocking)
        client = ModelManager.get_gemini_client()
        return client.models.generate_content(model=model, contents=prompt)


    # ----------------------------------------------------------------------
    # Async: OpenAI GPT-5
    # ----------------------------------------------------------------------
    @classmethod
    async def _openai_generate_answer_async(cls, prompt: str, model: str = "gpt-5"):
        """
        Internal async version for generating answers using OpenAI GPT-5.
        Runs blocking calls in executor to keep FastAPI async-safe.
        """
        client = ModelManager.get_openai_client()
        loop = asyncio.get_running_loop()

        fn = partial(
            client.chat.completions.create,
            model=model,
            messages=[{"role": "user", "content": prompt}],
        )
        return await loop.run_in_executor(None, fn)

    # ----------------------------------------------------------------------
    # Sync + Async friendly wrapper
    # ----------------------------------------------------------------------
    @classmethod
    def openai_generate_answer(cls, prompt: str, model: str = "gpt-5"):
        """
        Generate an answer from the OpenAI GPT-5 model.
        Works in both sync and async contexts.

        Args:
            prompt (str): The input prompt.
            model (str, optional): Model name to use. Defaults to "gpt-5".

        Returns:
            The response object from the OpenAI API.
        """
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                # Inside an async context
                return cls._openai_generate_answer_async(prompt, model)
        except RuntimeError:
            # No event loop -> sync context
            pass

        # Fallback: run sync (blocking)
        client = ModelManager.get_openai_client()
        return client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
        )
