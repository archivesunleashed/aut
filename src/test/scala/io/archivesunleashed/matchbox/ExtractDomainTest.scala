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
class ExtractDomainTest extends FunSuite {
  private val index = "index.html"
  private val umiacs = "www.umiacs.umd.edu"

  private val data1: Seq[(String, String)] = Seq.newBuilder.+=(
    ("http://www.umiacs.umd.edu/~jimmylin/", umiacs),
    ("https://github.com/lintool", "github.com"),
    ("http://ianmilligan.ca/2015/05/04/iipc-2015-slides-for-warcs-wats-and-wgets-presentation/", "ianmilligan.ca"),
    (index, null)).result()

  private val data2 = Seq.newBuilder.+=(
    (index, "http://www.umiacs.umd.edu/~jimmylin/", umiacs),
    (index, "lintool/", null)).result()

  test("simple") {
    data1.foreach {
      case (link, domain) => assert(ExtractDomain(link) == domain)
    }
  }

  test("withBase") {
    data2.foreach {
      case (link, base, domain) => assert(ExtractDomain(link, base) == domain)
    }
  }

  test("error") {
    assert(ExtractDomain(null) == null)
    assert(ExtractDomain(index, null) == null)
  }
}
