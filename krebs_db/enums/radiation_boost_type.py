import enum


class RadiationBoostType(enum.Enum):
    J = "J"         # Ja, mit Boost ohne nähere Angabe
    SIB = "SIB"     # Simultan integrierter Boost
    SEQ = "SEQ"     # Sequentieller Boost
    KON = "KON"     # Konkomitanter Boost
    N = "N"         # Nein, ohne Boost