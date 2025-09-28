# services/exceptions.py
class TaskCancelledException(Exception):
    """Custom exception to signal a graceful task cancellation."""
    pass