"""Named errors of the domain."""


class MalformedEventError(ValueError):
    """A record's bytes or fields do not describe a valid order event."""
