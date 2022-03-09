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

import io.archivesunleashed.matchbox.ExtractDate.DateComponent.{
  DD,
  MM,
  YYYY,
  YYYYMM,
  YYYYMMDD,
  YYYYMMDDHHMMSS
}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExtractDateTest extends FunSuite {

  test("Date extraction RDD") {
    val date = "20151204235402"
    val startSS = 0
    val yearSS = 4
    val monthSS = 6
    val daySS = 8
    val hourSS = 10
    val minuteSS = 12
    val secondSS = 14
    assert(ExtractDate(date, YYYY) == date.substring(startSS, yearSS))
    assert(ExtractDate(date, MM) == date.substring(yearSS, monthSS))
    assert(ExtractDate(date, DD) == date.substring(monthSS, daySS))
    assert(ExtractDate(date, YYYYMM) == date.substring(startSS, monthSS))
    assert(ExtractDate(date, YYYYMMDD) == date.substring(startSS, daySS))
    assert(
      ExtractDate(date, YYYYMMDDHHMMSS) == date.substring(startSS, secondSS)
    )
    // scalastyle:off null
    assert(ExtractDate(null, YYYYMMDDHHMMSS) == "")
    // scalastyle:on null
  }
}
