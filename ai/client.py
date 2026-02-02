"""AI client for OpenAI and Perplexity APIs."""

import logging
from typing import Any

import httpx

from config import settings
from utils.rate_limiter import rate_limiter

logger = logging.getLogger(__name__)


class AIClient:
    """Unified client for AI providers (OpenAI, Perplexity)."""

    def __init__(self):
        self._openai_client: httpx.AsyncClient | None = None
        self._perplexity_client: httpx.AsyncClient | None = None

    @property
    def openai_client(self) -> httpx.AsyncClient:
        """Get or create OpenAI client."""
        if self._openai_client is None:
            self._openai_client = httpx.AsyncClient(
                base_url="https://api.openai.com/v1",
                headers={
                    "Authorization": f"Bearer {settings.ai.openai_api_key}",
                    "Content-Type": "application/json",
                },
                timeout=60.0,
            )
        return self._openai_client

    @property
    def perplexity_client(self) -> httpx.AsyncClient:
        """Get or create Perplexity client."""
        if self._perplexity_client is None:
            self._perplexity_client = httpx.AsyncClient(
                base_url="https://api.perplexity.ai",
                headers={
                    "Authorization": f"Bearer {settings.ai.perplexity_api_key}",
                    "Content-Type": "application/json",
                },
                timeout=60.0,
            )
        return self._perplexity_client

    async def close(self) -> None:
        """Close all HTTP clients."""
        if self._openai_client:
            await self._openai_client.aclose()
            self._openai_client = None
        if self._perplexity_client:
            await self._perplexity_client.aclose()
            self._perplexity_client = None

    async def chat_completion(
        self,
        messages: list[dict[str, str]],
        model: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
        response_format: dict[str, str] | None = None,
    ) -> str:
        """
        Generate a chat completion using OpenAI.

        Args:
            messages: List of message dicts with 'role' and 'content'
            model: Model to use (defaults to config)
            max_tokens: Max tokens in response
            temperature: Sampling temperature
            response_format: Optional format specification

        Returns:
            Generated text response
        """
        if not settings.ai.openai_api_key:
            raise ValueError("OpenAI API key not configured")

        # Acquire rate limit
        await rate_limiter.acquire("openai")

        payload: dict[str, Any] = {
            "model": model or settings.ai.model,
            "messages": messages,
            "max_tokens": max_tokens or settings.ai.max_tokens,
            "temperature": temperature if temperature is not None else settings.ai.temperature,
        }

        if response_format:
            payload["response_format"] = response_format

        try:
            response = await self.openai_client.post(
                "/chat/completions",
                json=payload,
            )
            response.raise_for_status()
            data = response.json()

            return data["choices"][0]["message"]["content"]

        except httpx.HTTPStatusError as e:
            logger.error(f"OpenAI API error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"OpenAI request error: {e}")
            raise

    async def search_and_summarize(
        self,
        query: str,
        model: str = "llama-3.1-sonar-small-128k-online",
    ) -> dict[str, Any]:
        """
        Search the web and summarize using Perplexity.

        Args:
            query: Search query
            model: Perplexity model to use

        Returns:
            Dict with 'answer' and optionally 'citations'
        """
        if not settings.ai.perplexity_api_key:
            raise ValueError("Perplexity API key not configured")

        # Acquire rate limit
        await rate_limiter.acquire("perplexity")

        payload = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": query,
                }
            ],
        }

        try:
            response = await self.perplexity_client.post(
                "/chat/completions",
                json=payload,
            )
            response.raise_for_status()
            data = response.json()

            return {
                "answer": data["choices"][0]["message"]["content"],
                "citations": data.get("citations", []),
            }

        except httpx.HTTPStatusError as e:
            logger.error(f"Perplexity API error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Perplexity request error: {e}")
            raise

    async def generate_structured(
        self,
        prompt: str,
        system_prompt: str | None = None,
        model: str | None = None,
    ) -> str:
        """
        Generate structured JSON output.

        Args:
            prompt: User prompt
            system_prompt: Optional system prompt
            model: Model to use

        Returns:
            JSON string response
        """
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        return await self.chat_completion(
            messages=messages,
            model=model,
            response_format={"type": "json_object"},
        )

    async def generate_text(
        self,
        prompt: str,
        system_prompt: str | None = None,
        model: str | None = None,
        max_tokens: int | None = None,
    ) -> str:
        """
        Generate text response.

        Args:
            prompt: User prompt
            system_prompt: Optional system prompt
            model: Model to use
            max_tokens: Max tokens in response

        Returns:
            Text response
        """
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        return await self.chat_completion(
            messages=messages,
            model=model,
            max_tokens=max_tokens,
        )


# Global AI client instance
ai_client = AIClient()
