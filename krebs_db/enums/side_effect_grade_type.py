from enum import Enum


class SideEffectSeverityType(Enum):
    K = "K"  # keine (none)
    GRADE_1 = "1"  # mild
    GRADE_2 = "2"  # moderate
    GRADE_3 = "3"  # schwerwiegend
    GRADE_4 = "4"  # lebensbedrohlich
    GRADE_5 = "5"  # tödlich
    U = "U"  # unbekannt (unknown)