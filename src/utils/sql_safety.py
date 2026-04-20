"""
Helpers to prevent SQL injection in dynamically built DuckDB queries.

DuckDB supports positional parameters (?) for VALUES and some FROM-clause
functions (e.g. read_csv_auto, read_parquet, COPY TO), but NOT for SQL
identifiers (table / column names).  This module provides:

  • safe_identifier()  – whitelists table/column names against a strict
    alphanumeric pattern.
  • safe_path()        – rejects paths containing shell-dangerous characters.

For anything that is a *value* (file path, connection string, user query
text) prefer DuckDB parameterised queries with ``?``.
"""

import re

# Identifiers used in CREATE TABLE / SELECT must be simple ASCII.
# This deliberately excludes hyphens, spaces, dots, etc.  If a caller
# truly needs exotic names they must pre-quote them themselves — and
# accept the security responsibility.
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def safe_identifier(name: str, *, label: str = "identifier") -> str:
    """Return *name* if it looks like a safe SQL identifier, else raise.

    Parameters
    ----------
    name : str
        Candidate table or column name.
    label : str
        Human-readable label used in the error message.
    """
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Unsafe {label}: {name!r}. "
            f"Must match [a-zA-Z_][a-zA-Z0-9_]* (no spaces, dots, quotes, etc.)"
        )
    return name


# Characters that could break out of a file-path literal or be dangerous
# in a shell / file-system context.
_UNSAFE_PATH_CHARS = re.compile(r"['\";|&$`\\<>{}]")


def safe_path(path: str) -> str:
    """Return *path* if it doesn't contain obviously dangerous characters."""
    if _UNSAFE_PATH_CHARS.search(path):
        raise ValueError(
            f"Unsafe file path: {path!r}. "
            f"Contains characters that could break SQL or shell context."
        )
    return path
