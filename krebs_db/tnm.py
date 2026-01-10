# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

from sqlalchemy import Column, Integer, String, Boolean, DateTime, func

from krebs_db.base import Base
from krebs_db.utils import db_enum
from krebs_db.mixins import ReprMixin

from krebs_db.enums.tnm_version_type import TnmVersionType
from krebs_db.enums.cpu_prefix_type import CpuPrefixType
from krebs_db.enums.tnm_lymphatic_invasion_type import TnmLymphaticInvasionType
from krebs_db.enums.tnm_venous_invasion_type import TnmVenousInvasionType
from krebs_db.enums.tnm_perineural_invasion_type import TnmPerineuralInvasionType
from krebs_db.enums.tnm_surgical_margin_type import TnmSurgicalMarginType
from krebs_db.enums.uicc_stage_type import UiccStageType


class TNM(ReprMixin, Base):
    __tablename__ = "tnm"

    id = Column(Integer, primary_key=True)
    version = Column(db_enum(TnmVersionType), nullable=True, comment='TNM:Version')

    y_symbol = Column(Boolean, nullable=True, comment='TNM:y_Symbol')
    r_symbol = Column(Boolean, nullable=True, comment='TNM:r_Symbol')
    a_symbol = Column(Boolean, nullable=True, comment='TNM:a_Symbol')

    t_prefix = Column(db_enum(CpuPrefixType), nullable=True, comment='TNM:c_p_u_Praefix_T')
    t = Column(String, nullable=True, comment='TNM:T')

    m_symbol = Column(String, nullable=True, comment='TNM:m_Symbol')

    n_prefix = Column(db_enum(CpuPrefixType), nullable=True, comment='TNM:c_p_u_Praefix_N')
    n = Column(String, nullable=True, comment='TNM:N')

    m_prefix = Column(db_enum(CpuPrefixType), nullable=True, comment='TNM:c_p_u_Praefix_M')
    m = Column(String, nullable=True, comment='TNM:M')

    l = Column(db_enum(TnmLymphaticInvasionType), nullable=True, comment='TNM:L')
    v = Column(db_enum(TnmVenousInvasionType), nullable=True, comment='TNM:V')
    pn = Column(db_enum(TnmPerineuralInvasionType), nullable=True, comment='TNM:Pn')
    s = Column(db_enum(TnmSurgicalMarginType), nullable=True, comment='TNM:S')

    uicc_stage = Column(db_enum(UiccStageType), nullable=True, comment='TNM:UICC_Stadium')

    created_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
    updated_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())

