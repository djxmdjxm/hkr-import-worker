from enum import Enum


class OverallTumorStatusType(str, Enum):
    V = "V"  # Complete remission (CR)
    T = "T"  # Partial remission (PR)
    K = "K"  # No change / Stable disease (NC)
    P = "P"  # Progression
    D = "D"  # Divergent course
    B = "B"  # Minimal response (clinical improvement without PR criteria)
    R = "R"  # Complete remission with residual findings (CRr)
    Y = "Y"  # Recurrence
    U = "U"  # Assessment impossible
    X = "X"  # Data missing