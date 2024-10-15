from typing import Final


class PriorityConstants:
    INTERFACE_REQ: Final = 10  # Keep at low priority, many requests of this type
    SERIALIZATION_REQ: Final = 40  # This will prioritize serialization over heavy buffering
    ENTITY_SERIAL_REQ: Final = 30  # Better to split all the singular entity before processing, they are stored on file
    ENTITY_PACKAGE_REQ: Final = 50  # Validation is supposedly faster, prepare all for sending
    ENTITY_SEND_REQ: Final = 60  # If data is ready, send it, it was already regulated
    SYSTEM_REQ: Final = 70  # This has priority, usually needed to regulate traffic

    # Scraping priorities #
    COAUTHOR_REQ: Final = 12
    AUTHOR_REQ: Final = 13
    CIT_REQ: Final = 15
    PUB_REQ: Final = 16
