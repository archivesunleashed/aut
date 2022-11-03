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

/** Converts RFC 1123 dates to yyyyMMddHHmmss. */
object CovertLastModifiedDate {
  val months = Seq(
    "jan",
    "feb",
    "mar",
    "apr",
    "may",
    "jun",
    "jul",
    "aug",
    "sep",
    "oct",
    "nov",
    "dec"
  ).zipWithIndex.map { case (s, d) => (s, ("0" + (d + 1)).takeRight(2)) }

  /** Converts last_modified_date to yyyyMMddHHmmss.
    *
    * @param lastModifiedDate date returned by `getLastModified`, formatted as RFC 1123
    * @return last_modified_date as yyyyMMddHHmmss.
    */
  def apply(lastModifiedDate: String): String = {
    if (lastModifiedDate.isEmpty) {
      ""
    } else {
      // Credit: Helge Holzmann (@helgeho)
      // Adapted from https://github.com/archivesunleashed/aut/pull/547#issuecomment-1302094573
      val lc = lastModifiedDate.toLowerCase
      val date = months.find(m => lc.contains(m._1)).map(_._2).flatMap { m =>
        val d = lc
          .replace(":", "")
          .split(' ')
          .drop(1)
          .map(d => (d.length, d))
          .toMap
        for (y <- d.get(4); n <- d.get(2); t <- d.get(6))
          yield y + m + n + t
      }
      date match {
        case Some(date) =>
          date
        case None =>
          ""
      }
    }
  }
}
