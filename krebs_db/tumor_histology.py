# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

from sqlalchemy import Column, Integer, JSON, DateTime, ForeignKey, func

from krebs_db.base import Base
from krebs_db.utils import db_enum
from krebs_db.mixins import ReprMixin
from krebs_db.enums.icd_onc_morphologie_version_type import IcdOncMorphologieVersionType
from krebs_db.enums.histology_grading_type import HistologyGradingType


class TumorHistology(ReprMixin, Base):
    __tablename__ = 'tumor_histology'

    tumor_report_id = Column(Integer, ForeignKey("tumor_report.id"), primary_key=True)

    morphology_icd = Column(JSON, nullable=False, comment='Primaerdiagnose:Histologie:Morphologie_ICD_O') # { code: string, version: IcdOncMorphologieVersionType }
    grading = Column(db_enum(HistologyGradingType), nullable=False, comment='Primaerdiagnose:Histologie:Grading')
    lymph_nodes_examined = Column(Integer, nullable=True, comment='Primaerdiagnose:Histologie:LK_untersucht')
    lymph_nodes_affected = Column(Integer, nullable=True, comment='Primaerdiagnose:Histologie:LK_befallen')

    created_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
    updated_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
