/*
 * Copyright © 2017 The Archives Unleashed Project
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

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

/** Converts RFC 1123 dates to yyyyMMddHHmmss. */
object CovertLastModifiedDate {
  lazy val outputFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  /** Converts last_modified_date to yyyyMMddHHmmss.
    *
    * @param lastModifiedDate date returned by `getLastModified`, formatted as RFC 1123
    * @return last_modified_date as yyyyMMddHHmmss.
    */
  def apply(lastModifiedDate: String): String = {
    if (lastModifiedDate.isEmpty) {
      ""
    } else {
      val d = ZonedDateTime.parse(
        lastModifiedDate,
        DateTimeFormatter.RFC_1123_DATE_TIME
      )
      outputFormat.format(d)
    }
  }
}
