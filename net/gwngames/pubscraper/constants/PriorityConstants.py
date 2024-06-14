from typing import Final


class PriorityConstants:
    INTERFACE_REQ: Final = 10  # Keep at low priority, many requests of this type
    SERIALIZATION_REQ: Final = 20  # This will prioritize serialization over heavy buffering
    ENTITY_SERIAL_REQ: Final = 15  # Better to split all the singular entity before processing, they are stored on file
    SYSTEM_REQ: Final = 70  # This has priority, usually needed to regulate traffic
