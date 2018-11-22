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
class ExtractBoilerPipeTextTest extends FunSuite {
  val header = "HTTP/1.0 200 OK Content-Type: text/html;" +
   "charset=UTF-8 Expires: Fri, 20 Jul 2018 19:09:28 GMT Date:" +
   "Fri, 20 Jul 2018 19:09:28 GMT Cache-Control: private,;\r\n\r\n"
  var text = """<p>Text with a boiler plate.<p>
   <footer>Copyright 2017</footer>"""
  var boiler = """Copyright 2017"""

  test("Collects boilerpipe") {
    assert(ExtractBoilerpipeText(text) == boiler)
    // scalastyle:off null
    assert(ExtractBoilerpipeText(null) == "")
    // scalastyle:on null
    assert(ExtractBoilerpipeText("All Rights Reserved.") == "")
  }

  test("Removes Header information") {
    assert(ExtractBoilerpipeText(header + text) == boiler)
  }
}
