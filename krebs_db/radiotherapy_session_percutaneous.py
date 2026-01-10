# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

from sqlalchemy import Column, Integer, Boolean, DateTime, ForeignKey, func

from krebs_db.base import Base
from krebs_db.utils import db_enum
from krebs_db.mixins import ReprMixin
from krebs_db.enums.chemoradio_type import ChemoradioType


class RadiotherapySessionPercutaneous(ReprMixin, Base):
    __tablename__ = "radiotherapy_session_percutaneous"

    radiotherapy_session_id = Column(Integer, ForeignKey("radiotherapy_session.id"), primary_key=True)

    chemoradio = Column(db_enum(ChemoradioType), nullable=True, comment='Bestrahlung:Perkutan:Radiochemo')
    stereotactic = Column(Boolean, nullable=False, comment='Bestrahlung:Perkutan:Stereotaktisch')
    respiratory_gated = Column(Boolean, nullable=False, comment='Bestrahlung:Perkutan:Atemgetriggert')
    
    updated_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
    created_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
