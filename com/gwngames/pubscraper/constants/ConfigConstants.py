from typing import Final


class ConfigConstants:
    FAVORED_ORG: Final = "favored_org"
    CORE_PAGES_NUMBER: Final = "core_pages_number"
    AUTO_ADAPTIVE: Final = "auto_adaptive"
    BROWSER_EMBEDDED: Final = "browser_embedded"
    BAN_PENALTY: Final = "ban_penalty"
    MAX_ACTIVE_THREADS: Final = "max_active_threads"
    URL_TIMEOUT: Final = "url_timeout"
    CAPTCHA_API_KEY: Final = '2captcha_api_key'
    CAPTCHA_ACTION: Final = 'captcha_action'
    BROWSER_TYPE: Final = "browser_type"
    MIN_SECONDS_BEWTWEEN_UPDATES: Final = "min_seconds_between_updates"
    BROWSER_DATA_PATH: Final = "browser_data_path"
    BROWSER_DRIVER_PATH: Final = "browser_driver_path"
    DB_PREFIX: Final = "db_prefix"
    DB_HOST: Final = "db_host"
    DB_PORT: Final = "db_port"
    DB_USER: Final = "db_user"
    DB_PASSWORD: Final = "db_password"
    AUTHORS_REF: Final = "authors_ref"
    ROOT_AUTHORS: Final = "root_authors"
    MAX_LOGFILE_LINES: Final = "max_logfile_lines"
    INTERFACES_ENABLED: Final = 'interfaces_enabled'
    MAX_BUFFER_RETRIES: Final = 'max_buffer_retries'
    RETRY_TIME_SEC: Final = 'retry_time_sec'
    SERVER_URL: Final = 'server_url'
    SERVER_ENTITY_PORT: Final = 'entity_port'
    SERVER_STATUS_PORT: Final = 'status_port'
    MIN_WAIT_TIME: Final = 'min_wait_time'
    MAX_WAIT_TIME: Final = 'max_wait_time'
    MAX_MS_WORKTIME: Final = 'max_ms_worktime'
    MAX_IFACE_REQUESTS: Final = 'max_iface_requests'
    DEPTH_MAX: Final = 'depth_max'
    SHUFFLE_ROOTS: Final = 'shuffle_roots'
    DEBUG_DELAY: Final = 'debug_delay'
    RECOVERY_INST: Final = 'recovery_instance'


    # Actual constants
    LOG_FILENAME: Final = 'pubscraper.log'
