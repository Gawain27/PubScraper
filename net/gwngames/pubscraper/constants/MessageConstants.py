from typing import Final


class MessageConstants:
    #  OutSender messages
    MSG_SERIALIZE_DATA: Final = "serializeJsonData"
    MSG_SERIALIZE_ENTITY: Final = "serializeEntity"
    MSG_PACKAGE_ENTITY: Final = "validateEntity"
    MSG_SEND_ENTITY: Final = "sendEntity"

    #  Scraper messages -- General
    MSG_SCRAPE_TOPIC: Final = "scrapeTopic"
    MSG_SCRAPE_LEAF: Final = "scrapeLeaf"

    #  Scraper messages -- Interface
    MSG_ALL_SCHOLARLY_AUTHORS = "allScholarAuthor"
    MSG_ALL_DBLP_AUTHORS = "allDblpAuthor"
    MSG_ALL_SCIMAGO_JOURNALS = "allScimagoJournal"
    MSG_ALL_CORE_CONFERENCE = "allCoreConference"
    #  System messages
    MSG_UPDATE_LOAD_STATE: Final = "updateLoadState"
