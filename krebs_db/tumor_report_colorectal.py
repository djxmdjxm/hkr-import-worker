# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

from sqlalchemy import Column, Integer, ForeignKey, DateTime, func

from krebs_db.base import Base
from krebs_db.utils import db_enum
from krebs_db.mixins import ReprMixin
from krebs_db.enums.ras_mutation_type import RasMutationType

class TumorReportColorectal(ReprMixin, Base):
    __tablename__ = 'tumor_report_colorectal'

    tumor_report_id = Column(Integer, ForeignKey("tumor_report.id"), primary_key=True)

    ras_mutation = Column(db_enum(RasMutationType), nullable=True, comment='Modul_Darm:RASMutation')
    rectum_distance_anocutaneous_line_cm = Column(Integer, nullable=True, comment='Modul_Darm:RektumAbstandAnokutanlinie')

    created_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
    updated_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
