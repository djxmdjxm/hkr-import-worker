import enum

class ReportImportStatus(str, enum.Enum):
  Created            = 'created'
  Pending            = 'pending'
  Success            = 'success'
  SuccessWithWarnings = 'success_with_warnings'
  Failure            = 'failure'