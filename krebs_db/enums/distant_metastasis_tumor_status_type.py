from enum import Enum


class DistantMetastasisTumorStatusType(Enum):
    K = "K"  # keine Fernmetastasen nachweisbar
    T = "T"  # Fernmetastasen Residuen
    P = "P"  # Fernmetastasen Progress
    N = "N"  # Fernmetastasen No Change
    R = "R"  # neu aufgetretene Fernmetastase(n) bzw. Metastasenrezidiv
    F = "F"  # fraglicher Befund
    U = "U"  # unbekannt
    X = "X"  # fehlende Angabe