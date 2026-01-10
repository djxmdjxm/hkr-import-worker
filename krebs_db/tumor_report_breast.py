# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

from sqlalchemy import Column, Integer, ForeignKey, DateTime, func
from krebs_db.base import Base
from krebs_db.utils import db_enum
from krebs_db.mixins import ReprMixin
from krebs_db.enums.menopausal_status_type import MenopausalStatusType
from krebs_db.enums.receptor_status_type import ReceptorStatusType

class TumorReportBreast(ReprMixin, Base):
    __tablename__ = "tumor_report_breast"

    tumor_report_id = Column(Integer, ForeignKey("tumor_report.id"), primary_key=True)

    menopause_status_at_diagnosis = Column(db_enum(MenopausalStatusType), nullable=True, comment='Modul_Mamma:Praetherapeutischer_Menopausenstatus')
    estrogen_receptor_status = Column(db_enum(ReceptorStatusType), nullable=True, comment='Modul_Mamma:HormonrezeptorStatus_Oestrogen')
    progesterone_receptor_status = Column(db_enum(ReceptorStatusType), nullable=True, comment='Modul_Mamma:HormonrezeptorStatus_Progesteron')
    her2neu_status = Column(db_enum(ReceptorStatusType), nullable=True, comment='Modul_Mamma:Her2neuStatus')
    tumor_size_mm_invasive = Column(Integer, nullable=True, comment='Modul_Mamma:TumorgroesseInvasiv')
    tumor_size_mm_dcis = Column(Integer, nullable=True, comment='Modul_Mamma:TumorgroesseDCIS')

    created_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
    updated_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())

