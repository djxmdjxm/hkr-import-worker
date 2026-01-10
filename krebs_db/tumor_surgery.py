# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.


from sqlalchemy import Column, Integer, Date, DateTime, ForeignKey, JSON, func

from krebs_db.base import Base
from krebs_db.utils import db_enum
from krebs_db.mixins import ReprMixin
from krebs_db.enums.surgery_intent_type import SurgeryIntentType
from krebs_db.enums.residual_status_type import ResidualStatusType
from krebs_db.enums.date_accuracy_type import DateAccuracyType


class TumorSurgery(ReprMixin, Base):
    __tablename__ = "tumor_surgery"

    id = Column(Integer, primary_key=True)
    tumor_report_id = Column(Integer, ForeignKey("tumor_report.id"), nullable=False)

    intent = Column(db_enum(SurgeryIntentType), nullable=False, comment='OP:Intention')
    date = Column(Date, nullable=False, comment='OP:Datum')
    date_accuracy = Column(db_enum(DateAccuracyType), nullable=True, comment='OP:Datumsgenauigkeit')
    operations = Column(JSON, nullable=False, comment='OP:Menge_OPS')  # [{ code: string; version: string; }, ...]
    local_residual_status = Column(db_enum(ResidualStatusType), nullable=True, comment='OP:Lokale_Beurteilung_Residualstatus')

    created_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
    updated_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
    
    # - oBDS -> OP
    # tnm_id = Column(Integer, ForeignKey("tnm.id"), nullable=True, comment='TNM')
    # other_classification = Column(JSON, nullable=True, comment='Menge_Weitere_Klassifikation') # [{ date: Date, date_accuracy: string, name: string, stadium: string }, ...]
    # # NULL - unknown, [] - no complications
    # complications = Column(JSON, nullable=True, comment='Komplikationen') # [{ code: ComplicationCodeType; icd: { code: string; version: IcdVersionType } }, ...]
    # surgeon = Column(JSON, nullable=True, comment='Menge_Operateur') # [{ first_name?: string; last_name: string; is_primary: boolean }, ...]

    
