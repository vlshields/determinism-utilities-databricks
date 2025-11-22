"""
Totally unnecessary. You could also just use ValueError. 
But hey, life is about doing more work than necessary.
Just kidding, I'm a fan of custom exceptions for clarity.
"""
class MissingUniqueIdentifier(Exception):
    """Raised when a column expected to be unique contains duplicates"""
    pass

class IdentifierContainsNulls(Exception):
    """Raised when a column expected to be non-null contains nulls"""
    pass