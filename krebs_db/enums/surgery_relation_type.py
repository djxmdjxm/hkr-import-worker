import enum


class SurgeryRelationType(enum.Enum):
  O = "O"  # Ohne OP (Without surgery)
  A = "A"  # Adjuvant
  N = "N"  # Neoadjuvant
  I = "I"  # Interkurrent
  Z = "Z"  # Zwischen-/Intervallbestrahlung
  S = "S"  # Sonstiges
