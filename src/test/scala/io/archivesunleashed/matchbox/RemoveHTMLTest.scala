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

import java.io.IOException

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RemoveHTMLTest extends FunSuite {
  test("simple") {
    val html =
      """
      <html>
      <body>
      <div>Here is some...</div>
      <p>HTML</p>
      </body>
      </html>
      """

    val removed = RemoveHTML(html)
    assert(removed == "Here is some... HTML")
    // scalastyle:off null
    val empty = RemoveHTML(null)
    // scalastyle:on null
    assert(empty == "")
  }
}
