# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

from sqlalchemy import Column, Integer, Date, ForeignKey, JSON, DateTime, String, func
from sqlalchemy.orm import relationship

from krebs_db.base import Base
from krebs_db.utils import db_enum
from krebs_db.mixins import ReprMixin
from krebs_db.enums.metastasis_location_type import MetastasisLocationType
from krebs_db.enums.overall_tumor_status_type import OverallTumorStatusType
from krebs_db.enums.local_tumor_status_type import LocalTumorStatusType
from krebs_db.enums.lymph_node_tumor_status_type import LymphNodeTumorStatusType
from krebs_db.enums.distant_metastasis_tumor_status_type import DistantMetastasisTumorStatusType


class TumorFollowUp(ReprMixin, Base):
    __tablename__ = "tumor_follow_up"

    id = Column(Integer, primary_key=True)
    tumor_report_id = Column(Integer, ForeignKey("tumor_report.id"), nullable=False)

    tnm_id = Column(Integer, ForeignKey("tnm.id"), nullable=True, comment='Folgeereignis:TNM')
    other_classification = Column(JSON, nullable=True, comment='Folgeereignis:Menge_Weitere_Klassifikation') # [{ name: string, stadium: string }, ...]
    date = Column(Date, nullable=False, comment='Folgeereignis:Datum_Folgeereignis')
    date_accuracy = Column(String, nullable=False, comment='Folgeereignis:Datum_Folgeereignis:Datumsgenauigkeit')
    overall_tumor_status = Column(db_enum(OverallTumorStatusType), nullable=False, comment='Folgeereignis:Gesamtbeurteilung_Tumorstatus')
    local_tumor_status = Column(db_enum(LocalTumorStatusType), nullable=True, comment='Folgeereignis:Verlauf_Lokaler_Tumorstatus')
    lymph_node_tumor_status = Column(db_enum(LymphNodeTumorStatusType), nullable=True, comment='Folgeereignis:Verlauf_Tumorstatus_Lymphknoten')
    distant_metastasis_tumor_status = Column(db_enum(DistantMetastasisTumorStatusType), nullable=True, comment='Folgeereignis:Verlauf_Tumorstatus_Fernmetastasen')
    distant_metastasis = Column(JSON, nullable=True, comment='Folgeereignis:Menge_FM') # [{ location: MetastasisLocationType }, ...]

    updated_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
    created_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())

    # - oBDS_v3.0.0.8a_RKI_Schema -> Folgeereignis -> TNM
    tnm = relationship("TNM", foreign_keys=[tnm_id], uselist=False)

    # - oBDS -> Verlauf
    # general_performance_status = Column(Enum(PerformanceStatusType), nullable=False, comment='Allgemeiner_Leistungszustand')
