import enum


class SurgeryIntentType(enum.Enum):
    K = 'K'  # Kurativ
    P = 'P'  # Palliativ
    D = 'D'  # Diagnostisch
    R = 'R'  # Revision/Komplikation
    S = 'S'  # Sonstiges
    X = 'X'  # Keine Angabe