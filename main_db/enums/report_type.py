import enum

class ReportType(enum.Enum):
  # HKR (Hamburg) delivers XML in oBDS_RKI format — a tumor-centric variant of oBDS
  # defined by the ZfKD (Zentrum fuer Krebsregisterdaten) for national cancer reporting.
  # This enum value is stored in the database and used to route the import job
  # to the correct processor (rki_report_processor).
  # Version updated from 3.0.0.8a_RKI to 3.0.4_RKI in April 2024 (new schema from HKR).
  XML_oBDS_3_0_4_RKI = 'XML:oBDS_3.0.4_RKI'
