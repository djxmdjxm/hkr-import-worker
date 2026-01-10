import enum

class DateAccuracyType(enum.Enum):
    Exact = "E" # Exakt
    Day = "T"  # Tag (day)
    Month = "M"  # Monat (month)
    Estimate = "V"  # Vollschätzung (estimated)