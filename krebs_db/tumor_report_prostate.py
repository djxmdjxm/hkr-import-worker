# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

from sqlalchemy import Column, Integer, ForeignKey, Numeric, Date, DateTime, func

from krebs_db.base import Base
from krebs_db.utils import db_enum
from krebs_db.mixins import ReprMixin
from krebs_db.enums.gleason_score_result_type import GleasonScoreResultType
from krebs_db.enums.gleason_score_reason_type import GleasonScoreReasonType
from krebs_db.enums.date_accuracy_type import DateAccuracyType
from krebs_db.enums.gleason_grade_type import GleasonGradeType

class TumorReportProstate(ReprMixin, Base):
    __tablename__ = "tumor_report_prostate"

    tumor_report_id = Column(Integer, ForeignKey("tumor_report.id"), primary_key=True)

    gleason_primary_grade = Column(db_enum(GleasonGradeType), nullable=True, comment='Modul_Prostata:GleasonScore:GradPrimaer')
    gleason_secondary_grade = Column(db_enum(GleasonGradeType), nullable=True, comment='Modul_Prostata:GleasonScore:GradSekundaer')
    gleason_score_result = Column(db_enum(GleasonScoreResultType), nullable=True, comment='Modul_Prostata:GleasonScore:ScoreErgebnis')
    gleason_score_reason = Column(db_enum(GleasonScoreReasonType), nullable=True, comment='Modul_Prostata:AnlassGleasonScore')

    psa = Column(Numeric(10, 3), nullable=True, comment='Modul_Prostata:PSA')
    psa_date = Column(Date, nullable=True, comment='Modul_Prostata:DatumPSA')
    psa_date_accuracy = Column(db_enum(DateAccuracyType), nullable=True, comment='Modul_Prostata:DatumPSA:Datumsgenauigkeit')

    created_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
    updated_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
