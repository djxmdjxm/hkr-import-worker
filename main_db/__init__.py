import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .report_import import ReportImport

MAIN_DB_CONNECTION = os.environ['MAIN_DB_CONNECTION']

engine = create_engine(MAIN_DB_CONNECTION, echo=True)

# Create async session factory
Session = sessionmaker(bind=engine)
