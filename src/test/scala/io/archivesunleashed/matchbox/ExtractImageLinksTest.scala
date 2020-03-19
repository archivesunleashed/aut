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

import java.io.IOException

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExtractImageLinksRDDTest extends FunSuite {
  test("Extract simple image links RDD") {
    val fragment: String =
      """Image here: <img src="http://foo.bar.com/pic.png" alt="picture"> and another <img src="http://baz.org/a/b/banner.jpg" alt="baz banner"/>"""
    val extracted: Seq[(String, String, String)] = ExtractImageLinksRDD("", fragment)
    assert(extracted.size == 2)
    assert("http://foo.bar.com/pic.png" == extracted(0)._2)
    assert("picture" == extracted(0)._3)
    assert("http://baz.org/a/b/banner.jpg" == extracted(1)._2)
    assert("baz banner" == extracted(1)._3)
  }

  test("Extract relative image links RDD") {
    val fragment: String =
      """Image here: <img src="pic.png" alt="picture"> and another <img src="http://baz.org/a/b/banner.jpg" alt="baz banner" /> and <img src="../logo.gif" alt="LOGO" />"""
    val extracted: Seq[(String, String, String)] = ExtractImageLinksRDD("http://foo.bar.com/a/page.html", fragment)
    assert(extracted.size == 3)
    assert("http://foo.bar.com/a/pic.png" == extracted(0)._2)
    assert("picture" == extracted(0)._3)
    assert("http://baz.org/a/b/banner.jpg" == extracted(1)._2)
    assert("baz banner" == extracted(1)._3)
    assert("http://foo.bar.com/logo.gif" == extracted(2)._2)
    assert("LOGO" == extracted(2)._3)
  }

  test("Test image link errors RDD") {
    val fragment: String =
      """Image here: <img src="pic.png"> and another <img src="http://baz.org/a/b/banner.jpg"/> and <img src="../logo.gif"/>"""
    assert(ExtractImageLinksRDD("", "") == Nil)
    // Need way of creating an exception here
    // scalastyle:off null
    val invalid = null
    // scalastyle:on null
    intercept[IOException] {
      ExtractImageLinksRDD(invalid, fragment)
    }
  }
}
