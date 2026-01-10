import enum


class CpuPrefixType(enum.Enum):
    CLINICAL = 'c'
    PATHOLOGICAL = 'p'
    UNKNOWN = 'u'