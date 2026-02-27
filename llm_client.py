"""
Unified LLM Client
Supports both Anthropic Claude and OpenAI-compatible APIs with automatic fallback
"""

import os
from typing import List, Dict, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Track which client is available
_anthropic_available = False
_openai_available = False
_active_provider = None

# Try to import Anthropic
try:
    from anthropic import Anthropic
    _anthropic_available = True
except ImportError:
    _anthropic_available = False

# Try to import OpenAI
try:
    from openai import OpenAI
    _openai_available = True
except ImportError:
    _openai_available = False


def get_available_providers() -> List[str]:
    """Return list of available LLM providers"""
    providers = []
    if _anthropic_available and os.environ.get("ANTHROPIC_API_KEY"):
        providers.append("anthropic")
    if _openai_available and os.environ.get("OPENAI_API_KEY"):
        providers.append("openai")
    return providers


def get_active_provider() -> Optional[str]:
    """Return the currently active provider"""
    return _active_provider


def get_default_model() -> str:
    """Return the default model string for the highest-priority available provider"""
    providers = get_available_providers()
    if providers and providers[0] == "anthropic":
        return "claude-sonnet-4-20250514"
    return "openai/gpt-oss-120b"


def validate_env() -> bool:
    """Validate that at least one LLM provider is configured"""
    providers = get_available_providers()
    if providers:
        return True

    print("Warning: No LLM provider configured.")
    print("  For Claude: Set ANTHROPIC_API_KEY")
    print("  For OpenAI-compatible: Set OPENAI_API_KEY and OPENAI_BASE_URL")
    return False


def chat_completion(
    messages: List[Dict[str, str]],
    model: Optional[str] = None,
    max_tokens: int = 2000,
    temperature: float = 0.1,
    prefer_provider: Optional[str] = None
) -> Optional[str]:
    """
    Send a chat completion request to an LLM provider.

    Tries Anthropic Claude first (if available), then falls back to OpenAI-compatible API.

    Args:
        messages: List of message dicts with 'role' and 'content' keys
        model: Model name (provider-specific, or will use defaults)
        max_tokens: Maximum tokens in response
        temperature: Sampling temperature
        prefer_provider: Preferred provider ("anthropic" or "openai"), will try this first

    Returns:
        Response text string, or None if all providers fail
    """
    global _active_provider

    providers_to_try = []

    # Determine order of providers to try
    if prefer_provider == "openai":
        if _openai_available and os.environ.get("OPENAI_API_KEY"):
            providers_to_try.append("openai")
        if _anthropic_available and os.environ.get("ANTHROPIC_API_KEY"):
            providers_to_try.append("anthropic")
    else:
        # Default: try Anthropic first
        if _anthropic_available and os.environ.get("ANTHROPIC_API_KEY"):
            providers_to_try.append("anthropic")
        if _openai_available and os.environ.get("OPENAI_API_KEY"):
            providers_to_try.append("openai")

    if not providers_to_try:
        print("Error: No LLM provider available. Check your API keys.")
        return None

    last_error = None

    for provider in providers_to_try:
        try:
            if provider == "anthropic":
                result = _call_anthropic(messages, model, max_tokens, temperature)
                if result is not None:
                    _active_provider = "anthropic"
                    return result
            elif provider == "openai":
                result = _call_openai(messages, model, max_tokens, temperature)
                if result is not None:
                    _active_provider = "openai"
                    return result
        except Exception as e:
            last_error = e
            print(f"Provider {provider} failed: {e}")
            continue

    if last_error:
        print(f"All providers failed. Last error: {last_error}")
    return None


def _call_anthropic(
    messages: List[Dict[str, str]],
    model: Optional[str],
    max_tokens: int,
    temperature: float
) -> Optional[str]:
    """Call Anthropic Claude API"""

    # Default model for Anthropic
    if model is None or model.startswith("openai/"):
        model = "claude-sonnet-4-20250514"

    client = Anthropic()

    # Convert messages format if needed
    # Anthropic uses 'user' and 'assistant' roles, and system is separate
    system_message = None
    converted_messages = []

    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")

        if role == "system":
            system_message = content
        else:
            converted_messages.append({
                "role": role,
                "content": content
            })

    # Ensure we have at least one message
    if not converted_messages:
        return None

    # Build request kwargs
    kwargs = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": converted_messages,
    }

    # Only add temperature if not using extended thinking
    # (Claude's extended thinking doesn't support temperature)
    if temperature > 0:
        kwargs["temperature"] = temperature

    if system_message:
        kwargs["system"] = system_message

    response = client.messages.create(**kwargs)

    # Extract text from response
    if response.content and len(response.content) > 0:
        return response.content[0].text

    return None


def _call_openai(
    messages: List[Dict[str, str]],
    model: Optional[str],
    max_tokens: int,
    temperature: float
) -> Optional[str]:
    """Call OpenAI-compatible API"""

    # Default model for OpenAI
    if model is None:
        model = "openai/gpt-oss-120b"

    client = OpenAI()

    completion = client.chat.completions.create(
        model=model,
        messages=messages,
        max_tokens=max_tokens,
        temperature=temperature
    )

    if completion.choices and completion.choices[0].message.content:
        return completion.choices[0].message.content.strip()

    return None


# Convenience function for simple single-prompt calls
def ask(
    prompt: str,
    system: Optional[str] = None,
    model: Optional[str] = None,
    max_tokens: int = 2000,
    temperature: float = 0.1,
    prefer_provider: Optional[str] = None
) -> Optional[str]:
    """
    Simple interface for single-prompt LLM calls.

    Args:
        prompt: The user prompt
        system: Optional system message
        model: Model name (optional)
        max_tokens: Maximum tokens in response
        temperature: Sampling temperature
        prefer_provider: Preferred provider

    Returns:
        Response text string, or None if failed
    """
    messages = []

    if system:
        messages.append({"role": "system", "content": system})

    messages.append({"role": "user", "content": prompt})

    return chat_completion(
        messages=messages,
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
        prefer_provider=prefer_provider
    )
