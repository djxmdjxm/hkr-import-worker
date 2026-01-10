import enum

class RasMutationType(enum.Enum):
    WILDTYPE = 'W'     # Wildtyp
    MUTATED = 'M'      # Mutiert
    UNKNOWN = 'U'      # Unbekannt
    NOT_APPLICABLE = 'N'  # Keine Angabe / Nicht relevant