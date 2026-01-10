import enum


class CTCAEVersionType(enum.Enum):
    V4 = "4"              # CTCAE Version 4
    V4_03 = "4.03"        # CTCAE Version 4.03
    V5_0 = "5.0"          # CTCAE Version 5.0
    OTHER = "Sonstige"    # Other / older version