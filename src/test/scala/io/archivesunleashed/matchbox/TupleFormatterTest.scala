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
import shapeless._
import syntax.std.tuple._
// scalastyle:on underscore.import
import org.junit.runner.RunWith
import org.scalatest.{ FunSuite, Matchers }
import org.scalatest.junit.JUnitRunner

import ops.tuple.FlatMapper
import ops.tuple.ToList

@RunWith(classOf[JUnitRunner])
class TupleFormatterTest extends FunSuite with Matchers {
  test("tab delimit") {
    val tuple = (("ab", "bl", ("c", 9)), "d", 5, ("hi", 1))
    assert(TupleFormatter.tabDelimit(tuple) == "ab\tbl\tc\t9\td\t5\thi\t1")
    assert(TupleFormatter.tabDelimit.isInstanceOf[Poly1])
  }
  test("just flatten") {
    val tuple = ("an", 1, "cr", ("x", 3, ("NO", "YES")), "perhaps", "maybe", 3, (0,1))
    val flatTuple = ("an", 1, "cr", "x", 3, "NO", "YES", "perhaps", "maybe", 3, 0, 1)
    assert(TupleFormatter.flatten(tuple) == flatTuple)
    assert(TupleFormatter.flatten.isInstanceOf[TupleFormatter.LowPriorityFlatten])
    TupleFormatter.flatten.default shouldBe a[Poly1$CaseBuilder$$anon$1]
  }

  test ("Object extensions") {
    TupleFormatter.flatten shouldBe a[TupleFormatter.LowPriorityFlatten]
    TupleFormatter.tabDelimit shouldBe a[Poly1]
  }
}
