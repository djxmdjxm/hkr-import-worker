import enum


class RadionuclideTherapyType(enum.Enum):
  SIRT = 'SIRT'   # Selektive interne Radio-Therapie
  PRRT = 'PRRT'   # Peptid-Radio-Rezeptor-Therapie
  PSMA = 'PSMA'   # PSMA-Therapie
  RJT  = 'RJT'    # Radiojod-Therapie
  RIT  = 'RIT'    # Radioimmun-Therapie
