import enum


class DataExchangeConsentType(enum.Enum):
    OBJECTION = "1"         # informed and objected
    NO_OBJECTION = "2"      # informed, no objection
    NOT_INFORMED = "9"      # informierung unterblieb