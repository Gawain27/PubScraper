from typing import Final


class MessageConstants:
    #  OutSender messages
    MSG_SERIALIZE_DATA: Final = "serializeJsonData"
    MSG_SERIALIZE_ENTITY: Final = "serializeEntity"
    MSG_PACKAGE_ENTITY: Final = "validateEntity"
    MSG_SEND_ENTITY: Final = "sendEntity"

    #  Scraper messages
    MSG_GOOGLE_SCHOLAR_QUERY: Final = "getGoogleScholarEntity"

    #  System messages
    MSG_UPDATE_LOAD_STATE: Final = "updateLoadState"