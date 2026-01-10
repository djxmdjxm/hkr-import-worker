# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

from sqlalchemy import Column, Integer, ForeignKey, TIMESTAMP, func

from krebs_db.utils import db_enum
from krebs_db.base import Base
from krebs_db.mixins import ReprMixin
from krebs_db.enums.brachytherapy_type import BrachytherapyType
from krebs_db.enums.brachytherapy_dose_rate_type import BrachytherapyDoseRateType


class RadiotherapySessionBrachytherapy(ReprMixin, Base):
    __tablename__ = "radiotherapy_session_brachytherapy"

    radiotherapy_session_id = Column(Integer, ForeignKey("radiotherapy_session.id"), nullable=False, primary_key=True)

    type = Column(db_enum(BrachytherapyType), nullable=False, comment='Bestrahlung:Kontakt:Interstitiell_endokavitaer')
    dose_rate = Column(db_enum(BrachytherapyDoseRateType), nullable=True, comment='Bestrahlung:Kontakt:Rate_Type')

    created_at = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    updated_at = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
