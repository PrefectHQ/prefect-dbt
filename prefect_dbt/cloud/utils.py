"""Utility methods for common interactions with dbt Cloud API responses"""

from typing import Optional

from httpx import HTTPStatusError


def extract_user_message(ex: HTTPStatusError) -> Optional[str]:
    """
    Extracts user message from a error response from the dbt Cloud administrative API.

    Args:
        ex: An HTTPStatusError raised by httpx

    Returns:
        user_message from dbt Cloud administrative API response or None if a
        user_message cannot be extracted
    """
    response_payload = ex.response.json()
    status = response_payload.get("status", {})
    return status.get("user_message")
