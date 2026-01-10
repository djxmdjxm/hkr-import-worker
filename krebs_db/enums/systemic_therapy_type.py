import enum


class SystemicTherapyType(enum.Enum):
    CH = "CH" # Chemotherapie
    HO = "HO" # Hormontherapie
    IM = "IM" # Immun-/Antikörpertherapie
    ZS = "ZS" # zielgerichtete Substanzen
    CI = "CI" # Chemo- + Immun-/Antikörpertherapie
    CZ = "CZ" # Chemotherapie + zielgerichtete Substanzen
    CIZ = "CIZ" # Chemo- + Immun-/Antikörpertherapie + zielgerichtete Substanzen
    IZ = "IZ" # Immun-/Antikörpertherapie + zielgerichtete Substanzen
    SZ = "SZ" # Stammzelltransplantation (inkl. Knochenmarktransplantation)
    AS = "AS" # Active Surveillance
    WS = "WS" # Wait and see
    WW = "WW" # Watchful Waiting
    SO = "SO" # Sonstiges