import enum


class RadiotherapyEndReasonType(enum.Enum):
    E = "E"  # Reguläres Ende (Regular end)
    F = "F"  # Zieldosis erreicht mit Unterbrechung > 3 Kalendertage (Target dose reached with interruption > 3 days)
    A = "A"  # Abbruch wegen Nebenwirkungen (Termination due to side effects)
    P = "P"  # Abbruch wegen Progress (Termination due to disease progression)
    S = "S"  # Abbruch aus sonstigen Gründen (Termination for other reasons)
    V = "V"  # Patient verweigert weitere Therapie (Patient refuses further therapy)
    T = "T"  # Patient verstorben (Patient deceased)
    U = "U"  # Unbekannt (Unknown)