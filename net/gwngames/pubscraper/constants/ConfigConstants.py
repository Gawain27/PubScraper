from typing import Final


class ConfigConstants:
    # FIXME Deprecated, will be removed when implemented the automatic from link
    DELAY_THRESHOLD: Final = "delay_threshold"
    ROOT_AUTHORS: Final = "root_authors"
    MAX_LOGFILE_LINES: Final = "max_logfile_lines"
    LOG_FILENAME: Final = 'pubscraper.log'
    INTERFACES_ENABLED: Final = 'interfaces_enabled'
    MAX_BUFFER_RETRIES: Final = 'max_buffer_retries'
    ACCEPTABLE_LOAD: Final = 'acceptable_load'
    SERVER_URL: Final = 'server_url'
    SERVER_ENTITY_PORT: Final = 'entity_port'
    SERVER_STATUS_PORT: Final = 'status_port'
    KEEPDOWN_TIME: Final = 'keepdown_time'
    MIN_WAIT_TIME: Final = 'min_wait_time'
    MAX_WAIT_TIME: Final = 'max_wait_time'
    MAX_MS_WORKTIME: Final = 'max_ms_worktime'
    MAX_IFACE_REQUESTS: Final = 'max_iface_requests'
    MAX_FETCHABLE: Final = 'max_fetchable'
