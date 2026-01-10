# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

from sqlalchemy import Column, Integer, ForeignKey, TIMESTAMP, func

from krebs_db.base import Base
from krebs_db.utils import db_enum
from krebs_db.mixins import ReprMixin
from krebs_db.enums.radionuclide_therapy_type import RadionuclideTherapyType


class RadiotherapySessionMetabolic(ReprMixin, Base):
    __tablename__ = "radiotherapy_session_metabolic"

    radiotherapy_session_id = Column(Integer, ForeignKey("radiotherapy_session.id"), primary_key=True)

    type = Column(db_enum(RadionuclideTherapyType), nullable=False, comment='Metabolisch_Typ')

    created_at = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    updated_at = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
