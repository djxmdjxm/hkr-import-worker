import enum

class ReportType(enum.Enum):
    # oBDS_RKI 3.0.0.8a — historisches Schema, wird noch von aelteren Lieferungen verwendet
    XML_oBDS_3_0_0_8a_RKI = 'XML:oBDS_3.0.0.8a_RKI'
    # oBDS_RKI 3.0.4 — aktuelles Schema (HKR April 2024)
    XML_oBDS_3_0_4_RKI    = 'XML:oBDS_3.0.4_RKI'
