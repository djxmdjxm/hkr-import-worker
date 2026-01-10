import enum


class LocalTumorStatusType(enum.Enum):
    K = "K"  # No detectable tumor
    T = "T"  # Residual tumor (unknown if progressing or stable)
    P = "P"  # Residual tumor with progression
    N = "N"  # Residual tumor with no change
    R = "R"  # Local recurrence
    F = "F"  # Questionable finding
    U = "U"  # Unknown
    X = "X"  # Missing information
