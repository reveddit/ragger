class RaggerException(Exception):
    """Base exception for exceptions raised via this package."""

class BadSubmissionData(RaggerException):
    """Exception indicates submission data is not complete for at least one entry."""
