import enum


class DiagnosticCertaintyType(enum.Enum):
    UNKNOWN = '0'
    CLINICAL = '1'
    CLINICAL_IMAGING = '2'
    SPECIFIC_LAB = '4'
    CYTOLOGY = '5'
    HISTOLOGY_PRIM = '6'
    HISTOLOGY_MET = '7'
    AUTOPSY = '9'