# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

from sqlalchemy import Column, Integer, Numeric, Boolean, ForeignKey, DateTime, func

from krebs_db.base import Base
from krebs_db.mixins import ReprMixin

class TumorReportMelanoma(ReprMixin, Base):
    __tablename__ = "tumor_report_melanoma"

    tumor_report_id = Column(Integer, ForeignKey("tumor_report.id"), primary_key=True)

    tumor_thickness_mm = Column(Numeric(4, 1), nullable=True, comment='Modul_Malignes_Melanom:Tumordicke')
    ldh = Column(Numeric(10, 3), nullable=True, comment='Modul_Malignes_Melanom:Tumordicke')
    ulceration = Column(Boolean, nullable=True, comment='Modul_Malignes_Melanom:Ulzeration')

    created_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
    updated_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
