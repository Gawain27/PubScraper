from typing import Final


# Mainly used to describe phases, as each phase produces one/more objects of the same type
class EntityCidConstants:
    # Scholarly entities
    GOOGLE_SCHOLAR_AUTHOR: Final = 1000
    GOOGLE_SCHOLAR_COAUTHOR: Final = 1001  # Used to distinguish coauthors from authors, turns to GOOGLE_SCHOLAR_AUTHOR at end of its phase
    GOOGLE_SCHOLAR_PUB: Final = 1010
    GOOGLE_SCHOLAR_VERSION: Final = 1011
    GOOGLE_SCHOLAR_CIT: Final = 1020
    GOOGLE_SCHOLAR_ART: Final = 1030
    GOOGLE_SCHOLAR_ORG: Final = 1040
    GOOGLE_SCHOLAR_COAUTHORS: Final = 1050


