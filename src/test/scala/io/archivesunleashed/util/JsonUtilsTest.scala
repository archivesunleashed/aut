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

import io.archivesunleashed.util.JsonUtils
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JsonUtilsTest extends FunSuite {
  test("proper Map") {
    val map: Map[Symbol, Any] = Map('a -> 1, 'b -> 2, 'c -> 3)
    assert(JsonUtils.toJson(map) == """{"a":1,"b":2,"c":3}""")
  }

  test("any value") {
    val value = 12345
    assert(JsonUtils.toJson(value) == "12345")
  }

  test("json string") {
    val jsonString = """{"a":1,"b":2,"c":3}"""
    assert(JsonUtils.fromJson(jsonString) == Map("a" -> 1, "b" -> 2, "c" -> 3))
  }
}
