/*
 * Archives Unleashed Toolkit (AUT):
 * An open-source platform for analyzing web archives.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.archivesunleashed.matchbox

/** Gets different parts of a dateString. */
object ExtractDate {
  val start = 0
  val endyear = 4
  val endmonth = 6
  val endday = 8
  object DateComponent extends Enumeration {
    /** An enum specifying years, months, days or a combination. */
    type DateComponent = Value
    val YYYY, MM, DD, YYYYMM, YYYYMMDD = Value
  }
  import DateComponent._
  /** Extracts the wanted date component from a date.
    *
    * @param fullDate date returned by `WARecord.getCrawlDate`, formatted as YYYYMMDD
    * @param dateFormat an enum describing the portion of the date wanted
    */
  def apply(fullDate: String, dateFormat: DateComponent): String = {
    if (fullDate == null) { fullDate }
    else dateFormat match {
      case YYYY => fullDate.substring(start, endyear)
      case MM => fullDate.substring(endyear, endmonth)
      case DD => fullDate.substring(endmonth, endday)
      case YYYYMM => fullDate.substring(start, endmonth)
      case _ => fullDate.substring(start, endday)
    }
  }
}
