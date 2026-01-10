import enum


class ConsentToReportDKKR(enum.Enum):
    GUARDIAN_CONSENT = "1"  # EW liegt von/vom Sorgeberechtigten vor
    REFUSED = "2"           # EW wurde verweigert
    PENDING = "3"           # EW wird bald nachgereicht
    SELF_CONSENT = "5"      # EW liegt vom Patienten selbst vor