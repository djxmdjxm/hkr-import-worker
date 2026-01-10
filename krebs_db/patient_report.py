# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

from sqlalchemy import Column, String, Integer, Boolean, Date, TIMESTAMP, JSON, func
from sqlalchemy.orm import relationship

from krebs_db.utils import db_enum
from krebs_db.base import Base
from krebs_db.mixins import ReprMixin
from krebs_db.enums.register_type import RegisterType
from krebs_db.enums.date_accuracy_type import DateAccuracyType
from krebs_db.enums.gender_type import GenderType
from krebs_db.enums.icd_version_type import IcdVersionType

class PatientReport(ReprMixin, Base):
    __tablename__ = "patient_report"

    id = Column(Integer, primary_key=True)

    patient_id = Column(String, nullable=False, comment='Patient:Patient_ID')
    gender = Column(db_enum(GenderType), nullable=False, comment='Patienten_Stammdaten:Geschlecht')
    date_of_birth = Column(Date, nullable=False, comment='Patienten_Stammdaten:Geburtsdatum')
    date_of_birth_accuracy = Column(db_enum(DateAccuracyType), nullable=True, comment='Patienten_Stammdaten:Geburtsdatum:Datumsgenauigkeit')
    is_deceased = Column(Boolean, nullable=False, comment='Patienten_Stammdaten:Vitalstatus:Verstorben')
    vital_status_date = Column(Date, nullable=True, comment='Patienten_Stammdaten:Vitalstatus:Datum_Vitalstatus')
    vital_status_date_accuracy = Column(db_enum(DateAccuracyType), nullable=True, comment='Patienten_Stammdaten:Vitalstatus:Datumsgenauigkeit')
    death_causes = Column(JSON, nullable=True, comment='Patienten_Stammdaten:Vitalstatus:Todesursachen') # [{ code: string; version: IcdVersionType, is_underlying?: boolean }, ...]
    register = Column(db_enum(RegisterType), nullable=False, comment='Lieferregister')
    reported_at = Column(Date, nullable=False, comment='Lieferdatum')

    updated_at = Column(TIMESTAMP, nullable=False, server_default=func.now())
    created_at = Column(TIMESTAMP, nullable=False, server_default=func.now())

    # - oBDS_v3.0.0.8a_RKI_Schema -> Menge_Tumor
    tumor_reports = relationship("TumorReport")

    # - oBDS -> Patient -> Patienten_Stammdaten
    # first_names = Column(String, nullable=False)
    # last_name = Column(String, nullable=False)
    # title = Column(String, nullable=True)
    # name_suffix = Column(String, nullable=True)
    # name_prefix = Column(String, nullable=True)
    # birth_name = Column(String, nullable=True)
    # previous_names = Column(String, nullable=True)  # Array of strings — consider using PostgreSQL ARRAY type if needed
    # address = Column(JSON, nullable=False)
    # insurance_data = Column(JSON, nullable=False)