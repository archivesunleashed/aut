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

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RemoveHttpHeaderTest extends FunSuite {
  test("simple") {
    val header = "HTTP/1.1 200 OK\r\n\r\nHello content"
    val nohttp = "This has no Http"
    val removed = RemoveHttpHeader(header)
    val unchanged = RemoveHttpHeader(nohttp)
    val error = RemoveHttpHeader(null)
    assert(removed == "Hello content")
    assert(unchanged == nohttp)
    assert( error == null )
  }
}
