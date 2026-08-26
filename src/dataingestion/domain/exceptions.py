"""Domain exceptions for dataingestion service"""

class InvalidOrderError(Exception):
    """Raised when an order is invalid"""

class TransformationError(Exception):
    """Raised when a transformation fails"""

