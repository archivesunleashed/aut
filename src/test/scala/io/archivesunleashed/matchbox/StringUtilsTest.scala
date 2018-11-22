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
class StringUtilsTest extends FunSuite {
  test("remove prefix") {
    val s: String = "www.example.com"
    // scalastyle:off null
    val n: String = null
    // scalastyle:on null
    assert(s.removePrefixWWW() == "example.com")
    assert(n.removePrefixWWW() == "")
  }

  test("create hash") {
    val invalid: String = "A<B>C&D\"";
    // scalastyle:off null
    val except: String = null;
    // scalastyle:on null
    assert(invalid.escapeInvalidXML() == "A&lt;B&gt;C&amp;D&quot;");
    val caught = intercept[IOException] {except.escapeInvalidXML()}
    assert (caught.getMessage == "Caught exception processing input row ");
  }

  test ("md5 hash") {
    val s: String = "unesco.org";
    assert(s.computeHash() == "8e8decc8e8107bcf9d3896f3222b77d8");
  }
}
