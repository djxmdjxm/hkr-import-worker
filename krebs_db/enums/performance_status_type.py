import enum

class PerformanceStatusType(enum.Enum):
    ECOG_0 = "0"         # Normal activity (Karnofsky 90–100%)
    ECOG_1 = "1"         # Restricted strenuous activity, ambulatory (Karnofsky 70–80%)
    ECOG_2 = "2"         # Ambulatory >50% of the day, unable to work (Karnofsky 50–60%)
    ECOG_3 = "3"         # Limited self-care, >50% in bed/chair (Karnofsky 30–40%)
    ECOG_4 = "4"         # Completely disabled, bedridden (Karnofsky 10–20%)
    
    UNKNOWN = "U"        # Unknown

    KPS_10 = "10%"
    KPS_20 = "20%"
    KPS_30 = "30%"
    KPS_40 = "40%"
    KPS_50 = "50%"
    KPS_60 = "60%"
    KPS_70 = "70%"
    KPS_80 = "80%"
    KPS_90 = "90%"
    KPS_100 = "100%"