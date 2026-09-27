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

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExtractDomainTest extends FunSuite {
  private val index = "index.html"
  private val umiacs = "umd.edu"
  private val jimmylin = "http://www.umiacs.umd.edu/~jimmylin/"
  private val lintool = "https://github.com/lintool"
  private val github = "github.com"

  private val data1: Seq[(String, String)] = Seq.newBuilder
    .+=(
      (jimmylin, umiacs),
      (lintool, github),
      (
        "http://ianmilligan.ca/2015/05/04/iipc-2015-slides-for-warcs-wats-and-wgets-presentation/",
        "ianmilligan.ca"),
      (index, ""))
    .result()

  private val data2 = Seq.newBuilder
    .+=((index, jimmylin, umiacs), (lintool, jimmylin, github), (index, lintool, github))
    .result()

  private val data3 = Seq.newBuilder
    .+=(
      ("http://www.seetorontonow.canada-booknow.com\\booking_results.php", "canada-booknow.com"))
    .result()

  test("Extract simple domain extraction RDD") {
    data1.foreach { case (link, domain) =>
      assert(ExtractDomain(link) == domain)
    }
  }

  test("Test for domain errors RDD") {
    // scalastyle:off null
    assert(ExtractDomain(null) == "")
    assert(ExtractDomain("") == "")
    // scalastyle:on null
  }

  test("Test for domain backslash RDD") {
    data3.foreach { case (link, domain) =>
      assert(ExtractDomain(link) == domain)
    }
  }

  test("Extract domain with a supplied suffix list") {
    val suffixes = Set("com", "co.uk", "*.ck")
    assert(ExtractDomain("http://www.example.com/page", suffixes) == "example.com")
    assert(ExtractDomain("http://news.bbc.co.uk/", suffixes) == "bbc.co.uk")
    // "*.ck" makes any label under ck a public suffix, so the registrable domain keeps three labels.
    assert(ExtractDomain("http://www.foo.bar.ck/", suffixes) == "foo.bar.ck")
    // No matching suffix leaves the host unchanged.
    assert(ExtractDomain("http://intranet.local/", suffixes) == "intranet.local")
    assert(ExtractDomain(index, suffixes) == "")
  }

  test("Broadcast suffix list is reused per application and replaced for a new one") {
    def context(): SparkContext =
      new SparkContext(new SparkConf().setMaster("local[1]").setAppName("extract-domain"))

    val first = context()
    val firstBc =
      try {
        val bc = ExtractDomain.broadcast(first)
        assert(ExtractDomain.broadcast(first) eq bc)
        bc
      } finally {
        first.stop()
      }

    val second = context()
    try {
      val secondBc = ExtractDomain.broadcast(second)
      assert(secondBc ne firstBc)
      assert(secondBc.value == ExtractDomain.Suffixes)
    } finally {
      second.stop()
    }
  }
}
