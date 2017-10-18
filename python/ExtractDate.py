
# Used for getting different parts of a date
class DateComponent:
  YYYY = 0
  MM = 1
  DD = 2
  YYYYMM = 3
  YYYYMMDD = 4

def ExtractDate(fullDate, dateFormat):
  result = None
  if fullDate is None:
    return result
  if dateFormat == DateComponent.YYYY:
    result = fullDate[0:4]
  elif dateFormat == DateComponent.MM:
    result = fullDate[4:6]
  elif dateFormat == DateComponent.DD:
    result = fullDate[6:8]
  elif dateFormat == DateComponent.YYYYMM:
    result = fullDate[0:6]
  elif dateFormat == DateComponent.YYYYMMDD:
    result = fullDate[0:8]
  return result
