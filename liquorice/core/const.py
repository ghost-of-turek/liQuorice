import enum


@enum.unique
class TaskStatus(enum.Enum):
    NEW = enum.auto()
    PROCESSING = enum.auto()
    ERROR = enum.auto()
    DONE = enum.auto()

    def _generate_next_value_(name, start, count, last_values):
        return name
