"""The Unifier API client. The client knows no query parameters
(everything passes through to the server verbatim); the protocol is
documented by the server's OpenAPI document — call
`Unifier.help()` to fetch and print it over the internet.
"""

from .unifier import Unifier as unifier

__all__ = ['unifier']
