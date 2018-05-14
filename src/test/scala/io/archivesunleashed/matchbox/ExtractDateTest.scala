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

import io.archivesunleashed.matchbox.ExtractDate.DateComponent._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExtractDateTest extends FunSuite {

  val start: Int = 0
  val endyear: Int = 4
  val endmonth: Int = 6
  val endday: Int = 8

  test("simple") {
    assert(ExtractDate("20151204", YYYY) == "2015")
    assert(ExtractDate("20151204", MM) == "12")
    assert(ExtractDate("20151204", DD) == "04")
    assert(ExtractDate("20151204", YYYYMM) == "201512")
    assert(ExtractDate("20151204", YYYYMMDD) == "20151204")
    assert(ExtractDate(null, YYYYMMDD) == null)
  }

  test("more perfect") {
    assert(ExtractDate("20151204", YYYY) == "20151204".substring(start, endyear))
    assert(ExtractDate("20151204", MM) == "20151204".substring(endyear, endmonth))
    assert(ExtractDate("20151204", DD) == "20151204".substring(endmonth, endday))
    assert(ExtractDate("20151204", YYYYMM) == "20151204".substring(start, endmonth))
    assert(ExtractDate("20151204", YYYYMMDD) == "20151204".substring(start, endday))
  }
}
