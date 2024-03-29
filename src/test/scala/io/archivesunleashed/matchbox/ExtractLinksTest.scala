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

import scala.Byte
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class ExtractLinksTest extends FunSuite {

  val fragment: String =
    "Here is <a href=\"http://www.google.com\">a search engine</a>.\n" +
      "Here is <a href=\"http://www.twitter.com/\">Twitter</a>.\n"
  val fooFragment: String = "http://www.foobar.org/index.html"
  val url = "http://www.google.com"
  val twitter = "http://www.twitter.com/"
  val head = "a search engine"

  test("Extract simple links RDD") {
    val extracted: Seq[(String, String, String)] = ExtractLinks("", fragment)
    assert(extracted.size == 2)
    assert(url == extracted.head._2)
    assert(head == extracted.head._3)
    assert(twitter == extracted.last._2)
    assert("Twitter" == extracted.last._3)
  }

  test("Extract relative links with no src RDD") {
    val fragmentLocal: String = "Here is <a href=\"http://www.google.com\">" +
      "a search engine</a>.\nHere is a <a href=\"page.html\">a relative URL</a>.\n"
    val fooFragmentLocal = "http://www.foobar.org/page.html"
    val extracted: Seq[(String, String, String)] =
      ExtractLinks("", fragmentLocal)
    assert(extracted.size == 1)
    assert(url == extracted.head._2)
    assert(head == extracted.head._3)
  }

  test("Extract relative links with src RDD") {
    val fragmentLocal: String = "Here is <a href=\"http://www.google.com\">" +
      "a search engine</a>.\nHere is a <a href=\"page.html\">a relative URL</a>.\n"
    val fooFragmentLocal = "http://www.foobar.org/page.html"
    val extracted: Seq[(String, String, String)] =
      ExtractLinks(fooFragment, fragmentLocal)
    assert(extracted.size == 2)
    assert(url == extracted.head._2)
    assert(head == extracted.head._3)
    assert(fooFragmentLocal == extracted.last._2)
    assert("a relative URL" == extracted.last._3)
  }

  test("Test link errors RDD") {
    val bytes: Array[Byte] = "wronglyTyped".getBytes()
    val invalid: String =
      "Here is a fake url <a href=\"http://www.google.com\"> bogus search engine</a>."
    // scalastyle:off null
    assert(
      ExtractLinks(null, fragment) == mutable
        .MutableList[(String, String, String)]()
    )
    // scalastyle:on null
    assert(
      ExtractLinks("", "") == mutable
        .MutableList[(String, String, String)]()
    )
  }
}
