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

import io.archivesunleashed.df.{DetectLanguageDF, RemoveHTMLDF}
import com.google.common.io.Resources
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}


@RunWith(classOf[JUnitRunner])
class RecordDFTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-spark"
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    conf.set("spark.driver.allowMultipleContexts", "true");
    sc = new SparkContext(conf)
  }

  test("Keep valid pages DF") {
    val expected = "http://www.archive.org/"
    val base = RecordLoader.loadArchives(arcPath, sc)
      .all()
      .keepValidPagesDF()
      .take(1)(0)(1)
    assert (base.toString == expected)
  }

  test("Discard MIMEtypes DF") {
    val expected = "filedesc://IAH-20080430204825-00000-blackbook.arc"
    val mimeTypes = Set("text/html")
    val base = RecordLoader.loadArchives(arcPath, sc)
      .all()
      .discardMimeTypesDF(mimeTypes)
      .take(1)(0)(1)

    assert (base.toString == expected)
  }

  test("Discard date DF") {
    val expected = "20080430"
    val date = "20080429"
    val base = RecordLoader.loadArchives(arcPath, sc)
      .webpages()
      .discardDateDF(date)
      .take(1)(0)(0)

    assert (base.toString == expected)
  }

  test("Discard URLs DF") {
    val expected = "http://www.archive.org/index.php"
    val url = Set("http://www.archive.org/")
    val base1 = RecordLoader.loadArchives(arcPath, sc)
        .webpages()
        .discardUrlsDF(url)
        .take(1)(0)(1)

    val base2 = RecordLoader.loadArchives(arcPath, sc)
      .webgraph()
      .discardUrlsDF(url)
      .take(1)(0)(1)

    assert (base1.toString == expected)
    assert (base2.toString == expected)
  }

  test("Discard domains DF") {
    val expected1 = "http://www.hideout.com.br/"
    val domain = Set("www.archive.org")
    val base1 = RecordLoader.loadArchives(arcPath, sc)
      .webpages()
      .discardDomainsDF(domain)
      .take(1)(0)(1)

    val expected2 = "http://deadlists.com/deadlists/showresults.asp?KEY=12/16/78"
    val base2 = RecordLoader.loadArchives(arcPath, sc)
      .webgraph()
      .discardDomainsDF(domain)
      .take(1)(0)(1)

    assert(base1.toString == expected1)
    assert(base2.toString == expected2)
  }

  test("Discard HTTP status DF") {
    val expected = "200"
    val statusCode = Set("000")
    val base = RecordLoader.loadArchives(arcPath, sc)
      .all()
      .discardHttpStatusDF(statusCode)
      .take(1)(0)(6)

    assert (base.toString == expected)
  }

  test("Discard content DF") {
    val expected = "dns:www.archive.org"
    val contentRegex = Set("Content-Length: [0-9]{4}".r)
    val base = RecordLoader.loadArchives(arcPath, sc)
      .all()
      .select("url", "content")
      .discardContentDF(contentRegex)
      .take(2)(1)(0)

    assert (base.toString == expected)
  }

  test("Discard URL patterns DF") {
    val expected1 = "dns:www.archive.org"
    val urlRegex1 = Set(".*images.*".r)
    val base1 = RecordLoader.loadArchives(arcPath, sc)
      .all()
      .select("url")
      .discardUrlPatternsDF(urlRegex1)
      .take(2)(1)(0)

    val expected2 = "http://www.archive.org/details/DrinkingWithBob-MadonnaAdoptsAfricanBaby887"
    val urlRegex2 = Set(".*index.*".r)
    val base2 = RecordLoader.loadArchives(arcPath, sc)
      .webgraph()
      .select("src")
      .discardUrlPatternsDF(urlRegex2)
      .take(2)(1)(0)

    assert (base1.toString == expected1)
    assert (base2.toString == expected2)
  }

  test("Discard languages DF") {
    val expected = "dns:www.archive.org"
    val languages = Set("th","de","ht")
    val base = RecordLoader.loadArchives(arcPath, sc)
      .all()
      .select("url")
      .discardLanguagesDF(languages)
      .take(2)(1)(0)

    assert (base.toString == expected)
  }

  test("Keep HTTP status DF") {
    val expected = "http://www.archive.org/robots.txt"
    val statusCode = Set("200")
    val base = RecordLoader.loadArchives(arcPath, sc)
      .all()
      .keepHttpStatusDF(statusCode)
      .take(1)(0)(1)

    assert (base.toString == expected)
  }

  test("Keep date DF") {
    val expected = "http://www.archive.org/"
    val month = List("04")
    val base = RecordLoader.loadArchives(arcPath, sc)
      .webpages()
      .keepDateDF(month,"MM")
      .take(1)(0)(1)

    assert (base.toString == expected)
  }

  test("Keep URLs DF") {
    val expected = "http://www.archive.org/"
    val url = Set("http://www.archive.org/")
    val base1 = RecordLoader.loadArchives(arcPath, sc)
      .webpages()
      .keepUrlsDF(url)
      .take(1)(0)(1)

    val base2 = RecordLoader.loadArchives(arcPath, sc)
      .webgraph()
      .keepUrlsDF(url)
      .take(1)(0)(1)

    assert (base1.toString == expected)
    assert (base2.toString == expected)
  }

  test("Keep domains DF") {
    val expected1 = "http://www.archive.org/robots.txt"
    val domain = Set("www.archive.org")
    val base1 = RecordLoader.loadArchives(arcPath, sc)
      .all()
      .keepDomainsDF(domain)
      .take(1)(0)(1)

    val expected2 = "http://www.archive.org/"
    val base2 = RecordLoader.loadArchives(arcPath, sc)
      .webgraph()
      .keepDomainsDF(domain)
      .take(1)(0)(1)

    assert (base1.toString == expected1)
    assert (base2.toString == expected2)
  }

  test("Keep MIMEtypes Tika DF") {
    val expected = "image/jpeg"
    val mimeType = Set("image/jpeg")
    val base = RecordLoader.loadArchives(arcPath, sc)
      .all()
      .keepMimeTypesTikaDF(mimeType)
      .take(1)(0)(2)

    assert (base.toString == expected)
  }

  test("Keep MIMEtypes DF") {
    val expected = "text/html"
    val mimeType = Set("text/html")
    val base = RecordLoader.loadArchives(arcPath, sc)
      .all()
      .keepMimeTypesDF(mimeType)
      .take(1)(0)(3)

    assert (base.toString == expected)
  }

  test("Keep content DF") {
    val expected = "http://www.archive.org/images/logoc.jpg"
    val contentRegex = Set("Content-Length: [0-9]{4}".r)
    val base = RecordLoader.loadArchives(arcPath, sc)
      .all()
      .select("url", "content")
      .keepContentDF(contentRegex)
      .take(1)(0)(0)

    assert (base.toString == expected)
  }

  test("Keep URL patterns DF") {
    val expected1 = "http://www.archive.org/images/go-button-gateway.gif"
    val urlRegex1 = Set(".*images.*".r)
    val base1 = RecordLoader.loadArchives(arcPath, sc)
      .all()
      .select("url")
      .keepUrlPatternsDF(urlRegex1)
      .take(2)(1)(0)

    val expected2 = "http://www.archive.org/index.php"
    val urlRegex2 = Set(".*index.*".r)
    val base2 = RecordLoader.loadArchives(arcPath, sc)
      .webgraph()
      .select("src")
      .keepUrlPatternsDF(urlRegex2)
      .take(2)(1)(0)

    assert (base1.toString == expected1)
    assert (base2.toString == expected2)
  }

  test("Keep languages DF") {
    val expected = "http://www.archive.org/images/logoc.jpg"
    val languages = Set("th","de","ht")
    val base = RecordLoader.loadArchives(arcPath, sc)
      .all()
      .select("url")
      .keepLanguagesDF(languages)
      .take(1)(0)(0)

    assert (base.toString == expected)
  }

  test("Keep images DF") {
    val expected = "image/jpeg"
    val base = RecordLoader.loadArchives(arcPath, sc)
      .all()
      .keepImagesDF()
      .select("mime_type_tika")
      .take(1)(0)(0)

    assert (base.toString == expected)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
