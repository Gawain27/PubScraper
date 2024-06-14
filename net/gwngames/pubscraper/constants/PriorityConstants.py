from typing import Final


class PriorityConstants:
    INTERFACE_REQ: Final = 10  # Keep at low priority, many requests of this type
    SYSTEM_REQ: Final = 70  # This has priority, usually needed to regulate traffic
