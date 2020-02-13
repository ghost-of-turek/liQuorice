class LiquoriceException(Exception):
    ...


class DuplicateTaskError(LiquoriceException):
    ...


class WorkerError(LiquoriceException):
    ...


class JobError(LiquoriceException):
    ...


class RetryableError(JobError):
    pass


def format_exception(exc: Exception) -> str:
    return f'{exc.__class__.__name__}(\'{exc}\')'
