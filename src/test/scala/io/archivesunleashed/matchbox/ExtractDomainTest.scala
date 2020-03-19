/*
 * Copyright © 2017 The Archives Unleashed Project
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
class ExtractDomainRDDTest extends FunSuite {
  private val index = "index.html"
  private val umiacs = "www.umiacs.umd.edu"
  private val jimmylin = "http://www.umiacs.umd.edu/~jimmylin/"
  private val lintool = "https://github.com/lintool"
  private val github = "github.com"

  private val data1: Seq[(String, String)] = Seq.newBuilder.+=(
    (jimmylin, umiacs),
    (lintool, github),
    ("http://ianmilligan.ca/2015/05/04/iipc-2015-slides-for-warcs-wats-and-wgets-presentation/", "ianmilligan.ca"),
    (index, "")).result()

  private val data2 = Seq.newBuilder.+=(
    (index, jimmylin, umiacs),
    (lintool, jimmylin, github),
    (index, lintool, github)).result()

  private val data3 = Seq.newBuilder.+=(
    ("http://www.seetorontonow.canada-booknow.com\\booking_results.php", "www.seetorontonow.canada-booknow.com")).result()

  test("Extract simple domain extraction RDD") {
    data1.foreach {
      case (link, domain) => assert(ExtractDomainRDD(link) == domain)
    }
  }

  test("Extract domains with base RDD") {
    data2.foreach {
      case (link, base, domain) => assert(ExtractDomainRDD(link, base) == domain)
    }
  }

  test("Test for domain errors RDD") {
    // scalastyle:off null
    assert(ExtractDomainRDD(null) == "")
    assert(ExtractDomainRDD(index, null) == "")
    // scalastyle:on null
  }

  test("Test for domain backslash RDD") {
    data3.foreach {
      case (link, domain) => assert(ExtractDomainRDD(link) == domain)
    }
  }
}
