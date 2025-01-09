class UninmplementedCaptchaException(Exception):
    """Exception raised to signal that captcha type resolution is not implemented"""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
