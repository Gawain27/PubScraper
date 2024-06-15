from typing import Final


class PriorityConstants:
    INTERFACE_REQ: Final = 10  # Keep at low priority, many requests of this type
    SERIALIZATION_REQ: Final = 20  # This will prioritize serialization over heavy buffering
    ENTITY_SERIAL_REQ: Final = 15  # Better to split all the singular entity before processing, they are stored on file
    ENTITY_PACKAGE_REQ: Final = 25  # Validation is supposedly faster, prepare all for sending
    ENTITY_SEND_REQ: Final = 40  # If data is ready, send it, it was already regulated
    SYSTEM_REQ: Final = 70  # This has priority, usually needed to regulate traffic
