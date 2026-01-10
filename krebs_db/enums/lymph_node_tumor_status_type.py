from enum import Enum

class LymphNodeTumorStatusType(Enum):
    K = "K"  # kein Lymphknotenbefall nachweisbar
    T = "T"  # bekannter Lymphknotenbefall Residuen
    P = "P"  # bekannter Lymphknotenbefall Progress
    N = "N"  # bekannter Lymphknotenbefall No Change
    R = "R"  # neu aufgetretenes Lymphknotenrezidiv
    F = "F"  # fraglicher Befund
    U = "U"  # unbekannt
    X = "X"  # fehlende Angabe