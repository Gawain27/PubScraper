from typing import Final


# Mainly used to describe phases, as each phase produces one/more objects of the same type
class EntityCidConstants:
    CONFERENCE = 1040
    JOURNAL: Final = 1030
    AUTHOR: Final = 1000
    COAUTHOR: Final = 1000  # Used to distinguish coauthors from authors, turns to AUTHOR at end of its phase
    PUB: Final = 1010
    VERSION: Final = 1011
    CIT: Final = 1020

