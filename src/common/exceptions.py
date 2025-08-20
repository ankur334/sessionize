class SessionizeException(Exception):
    pass


class ConfigurationError(SessionizeException):
    pass


class ExtractionError(SessionizeException):
    pass


class TransformationError(SessionizeException):
    pass


class SinkError(SessionizeException):
    pass


class PipelineError(SessionizeException):
    pass


class SparkInitializationError(SessionizeException):
    pass


class ValidationError(SessionizeException):
    pass


class ComponentLoadError(SessionizeException):
    pass