import enum


class LateralityType(enum.Enum):
    LEFT = 'L'
    RIGHT = 'R'
    BILATERAL = 'B'
    MIDDLE = 'M'
    UNKNOWN = 'U'
    NOT_APPLICABLE = 'T'