"""
Marveen AuthZ — Permission Matrix & Check

Permission matrix governing which message types each agent pair may exchange.
"""

PERMISSIONS_MATRIX = {
    "dev": {
        "general": ["TASK_DELEGATION", "REQUEST_DATA", "STATUS_UPDATE", "ALERT", "INFO"],
        "research": ["REQUEST_DATA", "INFO"],
        "study": ["REQUEST_DATA", "INFO"],
        "gf": ["INFO"],
    },
    "general": {
        "dev": ["TASK_DELEGATION", "STATUS_UPDATE", "INFO", "ALERT"],
        "research": ["TASK_DELEGATION", "STATUS_UPDATE", "INFO"],
        "study": ["TASK_DELEGATION", "STATUS_UPDATE", "INFO"],
        "gf": ["INFO", "ALERT"],
    },
    "research": {
        "general": ["STATUS_UPDATE", "INFO"],
        "dev": ["INFO", "REQUEST_DATA"],
        "study": ["INFO"],
    },
    "study": {
        "general": ["STATUS_UPDATE", "INFO"],
        "dev": ["INFO", "REQUEST_DATA"],
    },
    "gf": {
        "general": ["INFO"],
        "dev": ["INFO"],
    },
}

ALLOWED_TYPES = [
    "TASK_DELEGATION",
    "REQUEST_DATA",
    "STATUS_UPDATE",
    "ALERT",
    "INFO",
]


class PermissionError(Exception):
    """Raised when an agent tries to send a disallowed message type."""
    pass


def check_permission(sender: str, to_agent: str, message_type: str = "INFO") -> bool:
    if sender == to_agent:
        return True
    if message_type not in ALLOWED_TYPES:
        raise PermissionError(f"Invalid message type: {message_type}")
    allowed = PERMISSIONS_MATRIX.get(sender, {}).get(to_agent)
    if allowed is None:
        raise PermissionError(f"Permission denied: {sender} cannot send {message_type} to {to_agent}")
    if message_type in allowed:
        return True
    raise PermissionError(f"Permission denied: {sender} cannot send {message_type} to {to_agent}")


__all__ = [
    "check_permission",
    "PermissionError",
    "PERMISSIONS_MATRIX",
    "ALLOWED_TYPES",
]
