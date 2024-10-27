from typing import Final


class PriorityConstants:
    INTERFACE_REQ: Final = 100  # Keep at low priority, many requests of this type
    ENTITY_SERIAL_REQ: Final = 30  # Adjust entity with correct metadata
    ENTITY_PACKAGE_REQ: Final = 31  # compression is done after serialization
    ENTITY_SEND_REQ: Final = 10  # If data is ready, send it asap, it's retained in memory
    SYSTEM_REQ: Final = 1  # This has priority, usually needed to regulate traffic

    # Scraping priorities # - Less authors, more data
    VERSION_REQ = 130
    JOURNAL_RANK_REQ = 122
    CONFERENCE_RANK_REQ = 121
    CONFERENCE_MAIN_RANK_REQ = 120
    JOURNAL_REQ: Final = 116
    CONFERENCE_REQ: Final = 115
    CIT_REQ: Final = 110
    PUB_REQ: Final = 105
    COAUTHOR_REQ: Final = 103
    AUTHOR_REQ: Final = 102


