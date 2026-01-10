import enum


class MetastasisLocationType(enum.Enum):
    PUL = 'PUL'  # Lung
    OSS = 'OSS'  # Bone
    HEP = 'HEP'  # Liver
    BRA = 'BRA'  # Brain
    LYM = 'LYM'  # Lymph nodes
    MAR = 'MAR'  # Bone marrow
    PLE = 'PLE'  # Pleura
    PER = 'PER'  # Peritoneum
    ADR = 'ADR'  # Adrenal glands
    SKI = 'SKI'  # Skin
    OTH = 'OTH'  # Other
    GEN = 'GEN'  # Genitals