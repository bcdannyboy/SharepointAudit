class SharePointAuditError(Exception):
    """Base exception class for the SharePoint Audit utility."""

    pass


class APIError(SharePointAuditError):
    """Raised for errors related to SharePoint or Graph API calls."""

    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code


class SharePointAPIError(APIError):
    """Raised for specific errors from the SharePoint API."""

    pass


class GraphAPIError(APIError):
    """Raised for specific errors from the Microsoft Graph API."""

    pass


class ConfigError(SharePointAuditError):
    """Raised for configuration-related errors."""

    pass


class DatabaseError(SharePointAuditError):
    """Raised for database-related errors."""

    pass


class CircuitBreakerOpenError(SharePointAuditError):
    """Raised when a circuit breaker is open and preventing an operation."""

    pass


class MaxRetriesExceededError(APIError):
    """Raised when an API operation fails after the maximum number of retries."""

    pass
