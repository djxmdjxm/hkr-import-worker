import enum


class GeneticVariantExpressionType(enum.Enum):
    M = "M"  # Mutation/positiv
    W = "W"  # Wildtyp/nicht mutiert/negativ
    P = "P"  # Polymorphismus
    N = "N"  # nicht bestimmbar
    U = "U"  # unbekannt