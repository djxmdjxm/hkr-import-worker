from enum import Enum


class SystemicTherapyEndReason(Enum):
    E = "E"   # reguläres Ende
    R = "R"   # reguläres Ende mit Dosisreduktion
    W = "W"   # reguläres Ende mit Substanzwechsel
    A = "A"   # Abbruch wegen Nebenwirkungen
    P = "P"   # Abbruch wegen Progress
    S = "S"   # Abbruch aus sonstigen Gründen
    V = "V"   # Patient verweigert weitere Therapie
    T = "T"   # Patient verstorben
    U = "U"   # unbekannt