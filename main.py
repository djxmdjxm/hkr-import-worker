# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

import asyncio
import signal
import os

from bullmq import Worker, Job
from sqlalchemy import select

from common.logging import getLogger

from main_db import Session, ReportImport
from main_db.enums.report_import_status import ReportImportStatus
from main_db.enums.report_type import ReportType

from processor import rki_report_processor
from processor.rki_report_processor import XsdValidationError

logger = getLogger(f'import_worker')


JOB_QUEUE_NAME = os.environ['JOB_QUEUE_NAME']
JOB_QUEUE_CONNECTION = os.environ['JOB_QUEUE_CONNECTION']

async def process(job: Job, job_token):
    logger.info(f'received job:{job.name} with data:{job.data}')

    try:
        match job.name:
            case 'report-import':
                uid = job.data['uid']
                if uid is not None:
                    return await process_report_import(uid)
                else:
                    logger.error('expected uid not to be None for "report-import" job')
            case _:
                logger.warning(f'unsupported job with name:{job.name} received')
    except Exception as e:
        logger.error(e)
    except:
        logger.error('something went wrong running the job')


async def process_report_import(uid: str):
    with Session() as session:
        stmt = select(ReportImport).where(ReportImport.uid == uid).with_for_update()

        result = session.execute(stmt)
        report_import: ReportImport = result.scalar_one_or_none()

        if report_import is None:
            logger.warning(f'report import with uid:{uid} is not found')
            return
        
        if report_import.status != ReportImportStatus.Created:
            return
        
        report_import.status = ReportImportStatus.Pending
        
        warnings = []
        try:
            match report_import.type:
                case ReportType.XML_oBDS_3_0_4_RKI | ReportType.XML_oBDS_3_0_0_8a_RKI:
                    warnings = rki_report_processor.execute(
                        report_import.uid,
                        report_import.file,
                        report_type=report_import.type.value
                    ) or []
                case _:
                    logger.warning(f'report import with uid:{report_import.uid} and type:{report_import.type} is not supported')
        except XsdValidationError as e:
            logger.error(
                "XSD validation failed",
                extra={
                    "category":          e.info_dict["category"],
                    "path":              e.info_dict["path"],
                    "technical_message": e.info_dict["technical_message"],
                }
            )
            report_import.additional_info = e.info_dict
            report_import.status = ReportImportStatus.Failure
        except Exception as e:
            logger.error(e)
            report_import.status = ReportImportStatus.Failure
        else:
            if warnings:
                report_import.additional_info = {
                    "warning_count": len(warnings),
                    "warnings":      warnings,
                }
                report_import.status = ReportImportStatus.SuccessWithWarnings
                logger.info(f'import succeeded with {len(warnings)} warnings')
            else:
                report_import.status = ReportImportStatus.Success
        finally:
            session.commit()
        

async def main():
    # Create an event that will be triggered for shutdown
    shutdown_event = asyncio.Event()

    def signal_handler(signal, frame):
        logger.info('signal received, shutting down...')
        shutdown_event.set()

    # Assign signal handlers to SIGTERM and SIGINT
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Feel free to remove the connection parameter, if your redis runs on localhost
    worker = Worker(JOB_QUEUE_NAME, process, {"connection": JOB_QUEUE_CONNECTION})

    logger.info("import-worker is up and running")

    # Wait until the shutdown event is set
    await shutdown_event.wait()

    # close the worker
    logger.info("cleaning up worker...")
    await worker.close()
    logger.info("worker shut down successfully")


if __name__ == "__main__":
    asyncio.run(main())