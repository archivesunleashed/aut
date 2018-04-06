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
import scala.collection.mutable
import java.io.IOException

@RunWith(classOf[JUnitRunner])
class ExtractLinksTest extends FunSuite {
  test("simple") {
    val fragment: String = "Here is <a href=\"http://www.google.com\">a search engine</a>.\n" + "Here is <a href=\"http://www.twitter.com/\">Twitter</a>.\n"
    val extracted: Seq[(String, String, String)] = ExtractLinks("", fragment)
    assert(extracted.size == 2)
    assert("http://www.google.com" == extracted.head._2)
    assert("a search engine" == extracted.head._3)
    assert("http://www.twitter.com/" == extracted.last._2)
    assert("Twitter" == extracted.last._3)
  }

  test("relative") {
    val fragment: String = "Here is <a href=\"http://www.google.com\">a search engine</a>.\n" + "Here is <a href=\"page.html\">a relative URL</a>.\n"
    val extracted: Seq[(String, String, String)] = ExtractLinks("", fragment, "http://www.foobar.org/index.html")
    assert(extracted.size == 2)
    assert("http://www.google.com" == extracted.head._2)
    assert("a search engine" == extracted.head._3)
    assert("http://www.foobar.org/page.html" == extracted.last._2)
    assert("a relative URL" == extracted.last._3)
  }

  test("errors") {
    val fragment: String = "Here is <a href=\"http://www.google.com\">a search engine</a>.\n" + "Here is <a href=\"page.html\">a relative URL</a>.\n"
    val invalid: String = "Here is a fake url <a href=\"http://www.google.com\"> bogus search engine</a>."
    assert(ExtractLinks(null, fragment, "http://www.foobar.org/index.html") == mutable.MutableList[(String, String, String)]())
    assert(ExtractLinks("", "", "http://www.foobar.org/index.html") == mutable.MutableList[(String, String, String)]())
    // invalid url should throw exception - need more information here
    intercept[IOException] { ExtractLinks("", null, "FROTSTEDwww.foobar.org/index.html") }

  }
}
