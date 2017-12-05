# Archives Unleashed Toolkit (AUT):
# An open-source platform for analyzing web archives.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
