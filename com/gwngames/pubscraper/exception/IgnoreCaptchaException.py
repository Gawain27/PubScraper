class IgnoreCaptchaException(Exception):
    """Exception raised to ignore captcha requests."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
