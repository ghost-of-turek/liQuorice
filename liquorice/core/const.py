import enum


@enum.unique
class TaskStatus(enum.Enum):
    NEW = enum.auto()
    PROCESSING = enum.auto()
    ERROR = enum.auto()
    DONE = enum.auto()
    RETRY = enum.auto()

    def _generate_next_value_(self, *args):
        return self
