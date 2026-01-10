import enum


class RadiotherapyIntentType(enum.Enum):
  K = "K"  # Curative (Kurativ)
  P = "P"  # Palliative
  O = "O"  # Other (Sonstige)
  S = "S"  # Symptom control (Symptomatisch)
  X = "X"  # Unknown / not specified
