from typing import Final


class MessageConstants:
    #  OutSender messages
    MSG_SERIALIZE_DATA: Final = "serializeJsonData"
    MSG_SERIALIZE_ENTITY: Final = "serializeEntity"
    MSG_PACKAGE_ENTITY: Final = "validateEntity"
    MSG_SEND_ENTITY: Final = "sendEntity"

    #  Scraper messages -- Interface
    MSG_SCHOLARLY_AUTHOR = "ScholarAuthor"
    MSG_DBLP_AUTHOR = "DblpAuthor"
    MSG_SCIMAGO_JOURNAL = "ScimagoJournal"
    MSG_CORE_CONFERENCE = "CoreConference"
