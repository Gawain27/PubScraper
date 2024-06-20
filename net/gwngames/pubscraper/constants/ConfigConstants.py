from typing import Final


class ConfigConstants:
    ROOT_TOPICS: Final = "root_topics"
    MAX_LOGFILE_LINES: Final = "max_logfile_lines"
    LOG_FILENAME: Final = 'pubscraper.log'
    INTERFACES_ENABLED: Final = 'interfaces_enabled'
    TERMS_MAX: Final = 'terms_max'
    DELAY_THRESHOLD: Final = 'delay_threshold'
    MAX_BUFFER_RETRIES: Final = 'max_buffer_retries'
    ACCEPTABLE_LOAD: Final = 'acceptable_load'
    SERVER_URL: Final = 'server_url'
    SERVER_ENTITY_PORT: Final = 'entity_port'
    SERVER_STATUS_PORT: Final = 'status_port'
    KEEPDOWN_TIME: Final = 'keepdown_time'
    TERMS_MIN: Final = 'terms_min'
    RESCRAPE_ROOT_TIME: Final = 'rescrape_root_time'
    MAX_SCHOLARLY_REQUESTS: Final = 'max_scholarly_requests'
