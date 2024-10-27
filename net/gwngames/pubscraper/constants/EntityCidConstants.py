from typing import Final


# Mainly used to describe phases, as each phase produces one/more objects of the same type
class EntityCidConstants:
    # Scholar entities
    GOOGLE_SCHOLAR_AUTHOR: Final = 1000
    GOOGLE_SCHOLAR_COAUTHOR: Final = 1001  # Used to distinguish coauthors from authors, turns to GOOGLE_SCHOLAR_AUTHOR at end of its phase
    GOOGLE_SCHOLAR_PUB: Final = 1010
    GOOGLE_SCHOLAR_VERSION: Final = 1011
    GOOGLE_SCHOLAR_CIT: Final = 1020
    GOOGLE_SCHOLAR_ART: Final = 1030
    GOOGLE_SCHOLAR_ORG: Final = 1040
    GOOGLE_SCHOLAR_COAUTHORS: Final = 1050

    # Dblp entities
    DBLP_PUBS: Final = 1100
    DBLP_JOURNAL: Final = 1110
    DBLP_CONFERENCE: Final = 1120

    # Scimago entities
    SCIMAGO_JOURNAL_RANK: Final = 1200

    # Core edu entities
    CORE_MAIN_CONFERENCE_RANK: Final = 1300
    CORE_CONFERENCE_RANK: Final = 1301


