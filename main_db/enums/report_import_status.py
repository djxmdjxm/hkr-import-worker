import enum

class ReportImportStatus(str, enum.Enum):
  Created = 'created'
  Pending = 'pending'
  Success = 'success'
  Failure = 'failure'