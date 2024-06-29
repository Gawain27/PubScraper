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
    MSG_SCHOLARLY_AUTHOR: Final = "scholarlyAuthor"
    MSG_SCHOLARLY_PUB: Final = "scholarlyPublication"
    MSG_GOOGLE_SCHOLAR_QUERY: Final = "getGoogleScholarEntity"
    MSG_SCHOLARLY_REL_ARTICLES = "getScholarlyPubRelatedArticles"
    MSG_SCHOLARLY_CITATIONS = "getScholarlyCitations"

    #  System messages
    MSG_UPDATE_LOAD_STATE: Final = "updateLoadState"
