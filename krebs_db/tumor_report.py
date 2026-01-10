# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.


from sqlalchemy import Column, Integer, String, Date, ForeignKey, DateTime, JSON, func
from sqlalchemy.orm import relationship

from krebs_db.base import Base
from krebs_db.utils import db_enum
from krebs_db.mixins import ReprMixin

from krebs_db.enums.date_accuracy_type import DateAccuracyType
from krebs_db.enums.icd_version_type import IcdVersionType
from krebs_db.enums.icd_onc_version_type import IcdOncVersionType
from krebs_db.enums.laterality_type import LateralityType
from krebs_db.enums.diagnostic_certainty_type import DiagnosticCertaintyType


class TumorReport(ReprMixin, Base):
    __tablename__ = "tumor_report"

    id = Column(Integer, primary_key=True)
    patient_report_id = Column(Integer, ForeignKey("patient_report.id"), nullable=False)

    tumor_id = Column(String, nullable=False, comment='Tumor_ID')

    # - oBDS_v3.0.0.8a_RKI_Schema -> Primaerdiagnose
    diagnosis_date = Column(Date, nullable=False, comment='Primaerdiagnose:Diagnosedatum')
    diagnosis_date_accuracy = Column(db_enum(DateAccuracyType), nullable=False, comment='Primaerdiagnose:Diagnosedatum:Datumsgenauigkeit')
    incidence_location = Column(String, nullable=False, comment='Primaerdiagnose:Inzidenzort')
    icd = Column(JSON, nullable=False, comment='Primaerdiagnose:Primaertumor_ICD') # { code: string, version: IcdVersionType }
    topographie = Column(JSON, nullable=True, comment='Primaerdiagnose:Primaertumor_Topographie_ICD_O') # { icd_code: string, icd_version: IcdOncVersionType }
    diagnostic_certainty = Column(db_enum(DiagnosticCertaintyType), nullable=False, comment='Primaerdiagnose:Diagnosesicherung')
    c_tnm_id = Column(Integer, ForeignKey("tnm.id"), nullable=True, comment='Primaerdiagnose:cTNM')
    p_tnm_id = Column(Integer, ForeignKey("tnm.id"), nullable=True, comment='Primaerdiagnose:pTNM')
    distant_metastasis = Column(JSON, nullable=True, comment='Primaerdiagnose:Menge_FM') # [{ date: Date, date_accuracy: string, location: MetastasisLocationType }, ...]
    other_classification = Column(JSON, nullable=True, comment='Primaerdiagnose:Menge_Weitere_Klassifikation') # [{ date: Date, date_accuracy: string, name: string, stadium: string }, ...]
    laterality = Column(db_enum(LateralityType), nullable=False, comment='Primaerdiagnose:Seitenlokalisation')

    created_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
    updated_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())

    # - oBDS_v3.0.0.8a_RKI_Schema -> Primaerdiagnose -> cTNM
    c_tnm = relationship("TNM", foreign_keys=[c_tnm_id], uselist=False)
    # - oBDS_v3.0.0.8a_RKI_Schema -> Primaerdiagnose -> pTNM
    p_tnm = relationship("TNM", foreign_keys=[p_tnm_id], uselist=False)
    # - oBDS_v3.0.0.8a_RKI_Schema -> Primaerdiagnose -> Histology
    histology = relationship("TumorHistology", uselist=False)
    # - oBDS_v3.0.0.8a_RKI_Schema -> Primaerdiagnose -> Modul_Mamma
    breast = relationship("TumorReportBreast", uselist=False)
    # - oBDS_v3.0.0.8a_RKI_Schema -> Primaerdiagnose -> Modul_Darm
    colorectal = relationship("TumorReportColorectal", uselist=False)
    # - oBDS_v3.0.0.8a_RKI_Schema -> Primaerdiagnose -> Modul_Prostata
    prostate = relationship("TumorReportProstate", uselist=False)
    # - oBDS_v3.0.0.8a_RKI_Schema -> Primaerdiagnose -> Modul_Malignes_Melanom
    melanoma = relationship("TumorReportMelanoma", uselist=False)

    # - oBDS_v3.0.0.8a_RKI_Schema -> Menge_OP
    surgeries = relationship("TumorSurgery")
    # - oBDS_v3.0.0.8a_RKI_Schema -> Menge_ST
    radiotherapies = relationship("TumorRadiotherapy")
    # - oBDS_v3.0.0.8a_RKI_Schema -> Menge_SYST
    systemic_therapies = relationship("TumorSystemicTherapy")
    # - oBDS_v3.0.0.8a_RKI_Schema -> Menge_Folgeereignis
    follow_ups = relationship("TumorFollowUp")
    

    # - oBDS -> Tumorzuordnung
    # additional_items = Column(JSON, nullable=True, comment='Menge_Zusatzitem') # [{ date: Date; date_accuracy: DateAccuracyType; type: string; value: string; comment: string; }, ...]

    # - oBDS -> Pathologie
    # text = Column(String, nullable=True, comment='Primaertumor_Diagnosetext')
    # topographie_text = Column(String, nullable=True, comment='Primaertumor_Topographie_Freitext')
    # referrer = Column(JSON, nullable=True, comment='Einsender') # { TBD } // TODO: must be NOT NULL if type == 'pathological'
    # finding_text = Column(String, nullable=True, comment='Befundtext')
    # finding_id = Column(String, nullable=True, comment='Befund_ID')
    # -

    # - oBDS -> Diagnose
    # historic_tumors = Column(JSON, nullable=True, comment='Menge_Fruehere_Tumorerkrankung') # [{ text?: string; icd: { code: string; version: IcdVersionType }, year?: number }, ...]
    # general_performance_status = Column(Enum(PerformanceStatusType), nullable=False, comment='Allgemeiner_Leistungszustand')
    # -

    # - oBDS -> Modul_Allgemein
    # social_service_contact_date = Column(Date, nullable=True, comment='Sozialdienstkontakt')
    # psychooncology_contact_date = Column(Date, nullable=True, comment='Psychoonkologiekontakt')
    # clinical_trial_participation_date = Column(Date, nullable=True, comment='Studienteilnahme')

    # - oBDS -> Modul_DKKR
    # consent_to_report_dkkr = Column(Enum(ConsentToReportDKKR, name="consent_to_report_dkkr_type"), nullable=True, comment='Meldung_EW_DKKR')
    # data_exchange_consent = Column(Enum(DataExchangeConsentType, name="data_exchange_consent_type"), nullable=True, comment='Meldung_Austausch')
    # gpoh_therapystudies = Column(JSON, nullable=True, comment='GPOH_Therapiestudienpatient') # [{ study_name: string, study_number?: string}, ...]
    # syndromes = Column(ARRAY(String), nullable=True, comment='Menge_Syndrome')
    # -
    
    # - oBDS -> Menge_Genetik
    # genetic_variant = relationship("TumorGeneticVariant", back_populates="tumor_report", uselist=False)
    # -
    
    # - oBDS -> Pathologie
    # pathology = relationship("TumorPathology", back_populates="tumor_report", uselist=False)
    
    # - oBDS -> Tod
    # death = relationship("TumorDeath", back_populates="tumor_report")
    
    # - oBDS -> Tumorkonferenz
    # board = relationship("TumorBoard", back_populates="tumor_report")


