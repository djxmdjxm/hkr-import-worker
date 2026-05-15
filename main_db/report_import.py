# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

from sqlalchemy import Column, Integer, DateTime, String, func, text
from sqlalchemy.dialects.postgresql import JSONB

from common.mixins import ReprMixin
from common.utils import db_enum

from .base import Base
from .enums.report_import_status import ReportImportStatus
from .enums.report_type import ReportType


class ReportImport(ReprMixin, Base):
    __tablename__ = "report_import"

    id = Column(Integer, primary_key=True)
    uid = Column(String, nullable=False, server_default=text("gen_random_uuid()"))

    status = Column(db_enum(ReportImportStatus), nullable=False)
    type = Column(db_enum(ReportType), nullable=False)
    file = Column(String, nullable=False)
    additional_info = Column(JSONB, nullable=True)

    created_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
    updated_at = Column(DateTime, nullable=False, server_default=func.current_timestamp())
