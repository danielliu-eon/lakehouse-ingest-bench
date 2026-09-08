"""How a rendered SQL script is assembled and taken apart again.

The renderer runs in the harness and the submitter runs inside the Flink
image, minutes apart and with nothing else in common, so the one thing they
must agree on lives here alone. This module imports nothing but the standard
library: it is copied into the image beside the submitter, where neither the
harness nor its dependencies exist.
"""

from __future__ import annotations

from collections.abc import Iterable

# A statement ends at a `;` closing its line, and that is what the submitter
# splits on. A property value may itself hold a `;` — a SASL configuration
# does — so the line end is what keeps the split unambiguous.
STATEMENT_TERMINATOR = ";\n"

# A blank line between statements, so a rendered script reads as a script.
# The gap belongs to the separator rather than to a statement, which is why
# splitting on the terminator alone still yields whole statements.
_STATEMENT_GAP = "\n"


def join_statements(statements: Iterable[str]) -> str:
    """The statements as one script, terminated so it can be split back apart."""
    return (STATEMENT_TERMINATOR + _STATEMENT_GAP).join(statements) + STATEMENT_TERMINATOR


def split_statements(script: str) -> list[str]:
    """The script's statements, in the order they have to be submitted."""
    return [statement.strip() for statement in script.split(STATEMENT_TERMINATOR) if statement.strip()]
