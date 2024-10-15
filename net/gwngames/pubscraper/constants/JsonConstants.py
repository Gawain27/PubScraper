from typing import Final


class JsonConstants:
    TAG_CITATION_GRAPH = "citation_graph"
    TAG_ENTITY: Final = "entity"
    TAG_ENTITY_CID: Final = "entity_cid"

    # Scholarly tags
    TAG_ORGANIZATION: Final = "organization"
    TAG_EMAIL_DOMAIN: Final = "email_domain"
    TAG_COAUTHORS: Final = "coauthors"
    TAG_PUBLICATIONS: Final = "publications"
    TAG_PUB_ID: Final = "author_pub_id"



    # server load constants
    TAG_DB_LOAD: Final = "db_load"
    TAG_CPU_LOAD: Final = "cpu_load"
    TAG_KEEPDOWN: Final = "keepdown"

    # constant for topic scraping
    TAG_TOPIC_SET: Final = "topic_set"
    TAG_TOPIC_BARRIER: Final = "topic_barrier"
