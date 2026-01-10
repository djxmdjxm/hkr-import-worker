# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

from sqlalchemy import Column, Integer, Date, ForeignKey, DateTime, func
from sqlalchemy.orm import relationship

from krebs_db.base import Base
from krebs_db.utils import db_enum
from krebs_db.mixins import ReprMixin
from krebs_db.enums.target_region_version import TargetRegionVersion
from krebs_db.enums.laterality_type import LateralityType
from krebs_db.enums.date_accuracy_type import DateAccuracyType


class RadiotherapySession(ReprMixin, Base):
  __tablename__ = "radiotherapy_session"

  id = Column(Integer, primary_key=True)
  tumor_radiotherapy_id = Column(Integer, ForeignKey("tumor_radiotherapy.id"), nullable=False)

  start_date = Column(Date, nullable=False, comment='Bestrahlung:Datum_Beginn_Bestrahlung')
  start_date_accuracy = Column(db_enum(DateAccuracyType), nullable=True, comment='Bestrahlung:Datum_Beginn_Bestrahlung:Datumsgenauigkeit')
  duration_days = Column(Integer, nullable=True, comment='Bestrahlung:Anzahl_Tage_ST_Dauer')
  target_area = Column(db_enum(TargetRegionVersion), nullable=True, comment='Bestrahlung:Applikationsart:Zielgebiet')
  laterality = Column(db_enum(LateralityType), nullable=True, comment='Bestrahlung:Applikationsart:Seite_Zielgebiet')

  updated_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
  created_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())

  # - oBDS_v3.0.0.8a_RKI_Schema -> Menge_ST -> Menge_Bestrahlung -> Bestrahlung -> Perkutan
  percutaneous = relationship("RadiotherapySessionPercutaneous", uselist=False)
  
  # - oBDS_v3.0.0.8a_RKI_Schema -> Menge_ST -> Menge_Bestrahlung -> Bestrahlung -> Kontakt
  brachytherapy = relationship("RadiotherapySessionBrachytherapy", uselist=False)

  # - oBDS_v3.0.0.8a_RKI_Schema -> Menge_ST -> Menge_Bestrahlung -> Bestrahlung -> Metabolisch
  metabolic = relationship("RadiotherapySessionMetabolic", uselist=False)

  # - oBDS -> ST -> Bestrahlung
  # end_date = Column(Date, nullable=False, comment='Ende')
  # radiation_type = Column(Enum(RadiationType), nullable=True, comment='Strahlenart')
  # total_dose = Column(JSON, nullable=True, comment='Applikationsart:Gesamtdosis')       # { "dose": float, "unit": "Gy" }
  # fraction_dose = Column(JSON, nullable=True, comment='Applikationsart:Einzeldosis')    # { "dose": float, "unit": "Gy" }
  # radiation_boost = Column(Enum(RadiationBoostType), nullable=True, comment='Boost')
  
  