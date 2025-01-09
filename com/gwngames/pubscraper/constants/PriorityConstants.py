from typing import Final


class PriorityConstants:
    INTERFACE_REQ: Final = 100  # Keep at low priority, many requests of this type
    ENTITY_SERIAL_REQ: Final = 30  # Adjust entity with correct metadata
    ENTITY_PACKAGE_REQ: Final = 31  # compression is done after serialization
    ENTITY_SEND_REQ: Final = 10  # If data is ready, send it asap, it's retained in memory

    # Scraping priorities # - Less authors, more data
    JOURNAL_REQ: Final = 102
    CONFERENCE_REQ: Final = 102
    PUB_REQ: Final = 101
    AUTHOR_REQ: Final = 102


