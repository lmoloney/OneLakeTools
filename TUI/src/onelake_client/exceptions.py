from __future__ import annotations


class OneLakeError(Exception):
    """Base exception for all OneLake client errors."""


class AuthenticationError(OneLakeError):
    """Failed to acquire or refresh a token."""


class NotFoundError(OneLakeError):
    """Resource not found (HTTP 404)."""

    def __init__(self, resource: str, message: str | None = None):
        self.resource = resource
        super().__init__(message or f"Not found: {resource}")


class PermissionDeniedError(OneLakeError):
    """Insufficient permissions (HTTP 403)."""


class RateLimitError(OneLakeError):
    """Rate limited (HTTP 429). Includes retry_after hint."""

    def __init__(self, retry_after: float | None = None, message: str | None = None):
        self.retry_after = retry_after
        super().__init__(message or f"Rate limited. Retry after {retry_after}s")


class FileTooLargeError(OneLakeError):
    """File exceeds the caller-specified size limit."""

    def __init__(self, size: int, max_bytes: int):
        self.size = size
        self.max_bytes = max_bytes
        super().__init__(f"File size {size} bytes exceeds limit of {max_bytes} bytes")


class ApiError(OneLakeError):
    """Catch-all for other API errors."""

    def __init__(self, status_code: int, body: str | None = None, message: str | None = None):
        self.status_code = status_code
        self.body = body
        super().__init__(message or f"API error {status_code}: {body}")
