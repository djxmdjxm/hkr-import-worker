# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

from sqlalchemy import Column, Integer, ForeignKey, DateTime, func

from sqlalchemy.orm import relationship
from krebs_db.utils import db_enum
from krebs_db.base import Base
from krebs_db.mixins import ReprMixin
from krebs_db.enums.radiotherapy_intent_type import RadiotherapyIntentType
from krebs_db.enums.surgery_relation_type import SurgeryRelationType


class TumorRadiotherapy(ReprMixin, Base):
    __tablename__ = "tumor_radiotherapy"

    id = Column(Integer, primary_key=True)
    tumor_report_id = Column(Integer, ForeignKey("tumor_report.id"), nullable=False)

    intent = Column(db_enum(RadiotherapyIntentType), nullable=True, comment='Intention')
    surgery_relation = Column(db_enum(SurgeryRelationType), nullable=True, comment='Stellung_OP')

    updated_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
    created_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())

    # - oBDS_v3.0.0.8a_RKI_Schema -> ST -> Menge_Bestrahlung
    sessions = relationship("RadiotherapySession")

    # - oBDS -> ST
    # report_reason = Column(Boolean, nullable=False, comment='Meldeanlass:True=behandlungsbeginn:False=behandlungsende')
    # end_reason = Column(Enum(RadiotherapyEndReasonType), nullable=True, comment='Ende_Grund')
    # side_effect_grade = Column(Enum(SideEffectSeverityType), nullable=True, comment='Nebenwirkung:Grad_maximal2_oder_unbekannt')
    # side_effects = Column(JSON, nullable=True, comment='Nebenwirkung:Menge_Nebenwirkung') # [{ type_lable: string; type_meddra_code: string; grade: SideEffectSeverityType; version: CTCAEVersionType }, ...]
