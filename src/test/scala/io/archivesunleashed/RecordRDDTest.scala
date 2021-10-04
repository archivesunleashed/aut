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

package io.archivesunleashed

import com.google.common.io.Resources
import io.archivesunleashed.matchbox.ExtractDate
import io.archivesunleashed.matchbox.ExtractDate.DateComponent
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class RecordRDDTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val badPath = Resources.getResource("arc/badexample.arc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-spark"
  private var sc: SparkContext = _
  private val archive = "http://www.archive.org/"
  private val sloan = "http://www.sloan.org"
  private val regex = raw"Please visit our website at".r

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    conf.set("spark.driver.allowMultipleContexts", "true");
    sc = new SparkContext(conf)
  }

  test("Expect no valid pages RDD") {
    val expectedLength = 0
    val base = RecordLoader
      .loadArchives(badPath, sc)
      .keepValidPages()
      .take(2)
    assert(base.length == expectedLength)
  }

  test("Expect no images RDD") {
    val expectedLength = 0
    val base = RecordLoader
      .loadArchives(badPath, sc)
      .keepValidPages()
      .take(2)
    assert(base.length == expectedLength)
  }

  test("Keep date RDD") {
    val testDate = "2008"
    val base = RecordLoader.loadArchives(arcPath, sc)
    val component = DateComponent.YYYY
    val r = base
      .filter(x => ExtractDate(x.getCrawlDate, component) == testDate)
      .map(mp => mp.getUrl)
      .take(3)
    val r2 = base
      .keepDate(List(testDate), component)
      .map(mp => mp.getUrl)
      .take(3)
    assert(r2.sameElements(r))
  }

  test("Keep HTTP status codes RDD") {
    val expected = 94
    val base = RecordLoader
      .loadArchives(arcPath, sc)
      .keepValidPages()
    val statusCodes: Set[String] = Set("200", "404")
    val r2 = base.keepHttpStatus(statusCodes).count
    assert(r2 == expected)
  }

  test("Keep URLs RDD") {
    val expected = 1
    val base = RecordLoader
      .loadArchives(arcPath, sc)
      .keepValidPages()
    val urls: Set[String] = Set(archive, sloan)
    val r2 = base.keepUrls(urls).count
    assert(r2 == expected)
  }

  test("Keep URL patterns RDD") {
    val expected = 1
    val base = RecordLoader
      .loadArchives(arcPath, sc)
      .keepValidPages()
    val urls = Set(archive.r, sloan.r, "".r)
    val r2 = base.keepUrlPatterns(urls).count
    assert(r2 == expected)
  }

  test("Keep domains RDD") {
    val expected = 91
    val base2 = RecordLoader
      .loadArchives(arcPath, sc)
      .keepValidPages()
    val urls: Set[String] = Set("archive.org", "sloan.org")
    val x2 = base2.keepDomains(urls).count()
    assert(x2 == expected)
  }

  test("Keep languages RDD") {
    val base2 = RecordLoader
      .loadArchives(arcPath, sc)
      .keepValidPages()
    val langs: Set[String] = Set("en", "fr")
    val r = Array("http://www.archive.org/", "http://www.archive.org/index.php")
    val r2 = base2
      .keepLanguages(langs)
      .map(r => r.getUrl)
      .take(2)
    assert(r2.sameElements(r))
  }

  test("Discard languages RDD") {
    val base2 = RecordLoader
      .loadArchives(arcPath, sc)
      .keepValidPages()
    val langs: Set[String] = Set("fr")
    val r = Array("http://www.archive.org/", "http://www.archive.org/index.php")
    val r2 = base2
      .discardLanguages(langs)
      .map(r => r.getUrl)
      .take(2)
    assert(r2.sameElements(r))
  }

  test("Keep MIMEtype Tika RDD") {
    val base = RecordLoader.loadArchives(arcPath, sc)
    val mime = Set("text/plain", "image/jpeg")
    val r2 = base
      .keepMimeTypesTika(mime)
      .map(mp => mp.getUrl)
      .take(3)
    assert(
      r2.deep == Array(
        "dns:www.archive.org",
        "http://www.archive.org/robots.txt",
        "http://www.archive.org/images/logoc.jpg"
      ).deep
    )
  }

  test("Keep MIMEtype RDD") {
    val base = RecordLoader.loadArchives(arcPath, sc)
    val mime = Set("text/plain", "image/jpeg")
    val r2 = base
      .keepMimeTypes(mime)
      .map(mp => mp.getUrl)
      .take(3)
    assert(
      r2.deep == Array(
        "filedesc://IAH-20080430204825-00000-blackbook.arc",
        "http://www.archive.org/robots.txt",
        "http://www.archive.org/images/logoc.jpg"
      ).deep
    )
  }

  test("Keep content RDD") {
    val expected = 1
    val base = RecordLoader
      .loadArchives(arcPath, sc)
      .keepValidPages()
    val regno = Set(regex, raw"UNINTELLIBLEDFSJKLS".r)
    val y2 = base.keepContent(Set(regex)).count()
    val y1 = base.keepContent(regno).count()
    assert(y2 == expected)
    assert(y1 == expected)
  }

  test("Discard MIMEtype RDD") {
    val base = RecordLoader.loadArchives(arcPath, sc)
    val mime = Set("text/plain", "image/jpeg")
    val r2 = base
      .discardMimeTypes(mime)
      .map(mp => mp.getUrl)
      .take(3)
    assert(
      r2.deep == Array(
        "dns:www.archive.org",
        archive,
        "http://www.archive.org/index.php"
      ).deep
    )
  }

  test("Discard MIMEtype Tika RDD") {
    val base = RecordLoader.loadArchives(arcPath, sc)
    val mime = Set("text/plain", "image/jpeg")
    val r2 = base
      .discardMimeTypesTika(mime)
      .map(mp => mp.getUrl)
      .take(3)
    assert(
      r2.deep == Array(
        "filedesc://IAH-20080430204825-00000-blackbook.arc",
        "http://www.archive.org/",
        "http://www.archive.org/index.php"
      ).deep
    )
  }

  test("Discard date RDD") {
    val base = RecordLoader.loadArchives(arcPath, sc)
    val date = "20080430"
    val r = base.filter(x => x.getCrawlDate != date).collect()
    val r2 = base.discardDate(date).take(3)
    assert(r.deep == Array().deep)
  }

  test("Discard URLs RDD") {
    val expected = 94
    val base = RecordLoader
      .loadArchives(arcPath, sc)
      .keepValidPages()
    val urls: Set[String] = Set(sloan)
    val r2 = base.discardUrls(urls).count()
    assert(r2 == expected)
  }

  test("Discard URL patterns RDD") {
    val expected = 93
    val base = RecordLoader
      .loadArchives(arcPath, sc)
      .keepValidPages()
    val urls = Set(archive.r, sloan.r, "".r)
    val r2 = base.discardUrlPatterns(urls).count
    assert(r2 == expected)
  }

  test("Discard HTTP status codes RDD") {
    val expected = 46
    val base = RecordLoader.loadArchives(arcPath, sc)
    val statusCodes: Set[String] = Set("200", "404")
    val r2 = base.discardHttpStatus(statusCodes).count
    assert(r2 == expected)
  }

  test("Discard domains RDD") {
    val expected = 94
    val base = RecordLoader
      .loadArchives(arcPath, sc)
      .keepValidPages()
    val urls: Set[String] = Set("sloan.org")
    val r2 = base.discardDomains(urls).count()
    assert(r2 == expected)
  }

  test("Discard content RDD") {
    val expected = 93
    val base = RecordLoader
      .loadArchives(arcPath, sc)
      .keepValidPages()
    val regno = Set(regex, raw"UNINTELLIBLEDFSJKLS".r)
    val y2 = base.discardContent(Set(regex)).count()
    val y1 = base.discardContent(regno).count()
    assert(y2 == expected)
    assert(y1 == expected)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
