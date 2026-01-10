import enum



class RadiationType(enum.Enum):
    UH = "UH"       # Photonen (ultraharte Röntgenstrahlen, inkl. Gamma-Strahler)
    EL = "EL"       # Elektronen
    NE = "NE"       # Neutronen
    PN = "PN"       # Protonen (leichte Wasserstoffionen)
    SI = "SI"       # Schwerionen (C12, O, He)
    RO = "RO"       # Weichstrahl (kV)
    Co_60 = "Co-60" # Cobalt-60
    SO = "SO"       # Sonstige

    # nuclides
    LU_177 = "Lu-177"      # Lu-177
    J_131 = "J-131"        # J-131
    Y_90 = "Y-90"          # Y-90
    RA_223 = "Ra-223"      # Ra-223
    AC_225 = "Ac-225"      # Ac-225
    SM_153 = "Sm-153"      # Sm-153
    TB_161 = "Tb-161"      # Tb-161
    SR_89 = "Sr-89"        # Sr-89
    IR_192 = "Ir-192"      # Ir-192
    SONU = "SONU"          # Sonstige Nuklide (Other nuclides)