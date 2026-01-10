import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .patient_report import PatientReport
from .tnm import TNM
from .tumor_report import TumorReport
from .tumor_histology import TumorHistology
from .tumor_report_breast import TumorReportBreast
from .tumor_report_colorectal import TumorReportColorectal
from .tumor_report_prostate import TumorReportProstate
from .tumor_report_melanoma import TumorReportMelanoma
from .tumor_surgery import TumorSurgery
from .tumor_radiotherapy import TumorRadiotherapy
from .radiotherapy_session import RadiotherapySession
from .radiotherapy_session_percutaneous import RadiotherapySessionPercutaneous
from .radiotherapy_session_brachytherapy import RadiotherapySessionBrachytherapy
from .radiotherapy_session_metabolic import RadiotherapySessionMetabolic
from .tumor_systemic_therapy import TumorSystemicTherapy
from .tumor_follow_up import TumorFollowUp

KREBS_DB_CONNECTION = os.environ['KREBS_DB_CONNECTION']

engine = create_engine(KREBS_DB_CONNECTION)
Session = sessionmaker(bind=engine)
