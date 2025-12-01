"""
Python-based transformations for PelagisFlow.

Transformations can be implemented as:
1. Function-based: implement a transform function
2. Class-based: inherit from AbstractTransformation and implement run() method

Transformations can include:
- Helper functions
- Helper classes
- Multiple files organized as a package
"""

from transformations.python.base import AbstractTransformation

__all__ = [
    "AbstractTransformation",
]
