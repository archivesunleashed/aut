/*
 * Archives Unleashed Toolkit (AUT):
 * An open-source toolkit for analyzing web archives.
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

// scalastyle:off underscore.import
import io.archivesunleashed.matchbox.ExtractDate.DateComponent._
// scalastyle:on underscore.import
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExtractDateTest extends FunSuite {

  test("simple") {
    val date = "20151204"
    assert(ExtractDate(date, YYYY) == "2015")
    assert(ExtractDate(date, MM) == "12")
    assert(ExtractDate(date, DD) == "04")
    assert(ExtractDate(date, YYYYMM) == "201512")
    assert(ExtractDate(date, YYYYMMDD) == date)
    // scalastyle:off null
    assert(ExtractDate(null, YYYYMMDD) == "")
    // scalastyle:on null
  }

  test("more perfect") {
    val date = "20151204"
    val startSS = 0
    val yearSS = 4
    val monthSS = 6
    val daySS = 8
    assert(ExtractDate(date, YYYY) == date.substring(startSS, yearSS))
    assert(ExtractDate(date, MM) == date.substring(yearSS, monthSS))
    assert(ExtractDate(date, DD) == date.substring(monthSS, daySS))
    assert(ExtractDate(date, YYYYMM) == date.substring(startSS, monthSS))
    assert(ExtractDate(date, YYYYMMDD) == date.substring(startSS, daySS))
  }
}
