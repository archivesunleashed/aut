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

import io.archivesunleashed.udfs.{
  detectLanguage,
  detectMimeTypeTika,
  extractDomain,
  removeHTML,
  hasContent,
  hasDate,
  hasDomains,
  hasHTTPStatus,
  hasImages,
  hasLanguages,
  hasMIMETypes,
  hasMIMETypesTika,
  hasUrlPatterns,
  hasUrls
}
import com.google.common.io.Resources
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
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
    val base = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .keepValidPagesDF()
      .take(1)(0)(1)

    assert(base.toString == expected)
  }

  test("Has HTTP Status") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val expected = "200"
    val base = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .select($"http_status_code")
      .filter(hasHTTPStatus($"http_status_code", lit(Array("200", "000"))))
      .take(1)(0)(0)

    assert(base.toString == expected)
  }

  test("Has URLs") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val expected1 = "http://www.archive.org/robots.txt"
    val expected2 = "http://www.archive.org/"
    val base1 = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .select($"url")
      .filter(
        hasUrls(
          $"url",
          lit(
            Array(
              "http://www.archive.org/",
              "http://www.archive.org/robots.txt"
            )
          )
        )
      )
      .take(1)(0)(0)

    val base2 = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .select($"url")
      .filter(hasUrls($"url", lit(Array("http://www.archive.org/"))))
      .take(1)(0)(0)

    assert(base1.toString == expected1)
    assert(base2.toString == expected2)
  }

  test("Has domains") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val expected = "http://www.archive.org/robots.txt"
    val base1 = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .select($"url")
      .filter(hasDomains(extractDomain($"url"), lit(Array("archive.org"))))
      .take(1)(0)(0)

    assert(base1.toString == expected)
  }

  test("Has MIME Types") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val expected = "text/html"
    val base = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .select($"mime_type_web_server")
      .filter(hasMIMETypes($"mime_type_web_server", lit(Array("text/html"))))
      .take(1)(0)(0)

    assert(base.toString == expected)
  }

  test("Has MIME Types Tika") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val expected = "text/html"
    val base = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .select($"mime_type_web_server")
      .filter(hasMIMETypesTika($"mime_type_tika", lit(Array("text/html"))))
      .take(1)(0)(0)

    assert(base.toString == expected)
  }

  test("Has Content") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val expected = "http://www.archive.org/images/logoc.jpg"
    val base = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .select($"url", $"content")
      .filter(hasContent($"content", lit(Array("Content-Length: [0-9]{4}"))))
      .take(1)(0)(0)

    assert(base.toString == expected)
  }

  test("Has URL Patterns") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val expected1 = "http://www.archive.org/images/go-button-gateway.gif"
    val base1 = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .select($"url")
      .filter(hasUrlPatterns($"url", lit(Array(".*images.*"))))
      .take(2)(1)(0)

    val expected2 = "http://www.archive.org/index.php?skin=classic"
    val base2 = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .select($"url")
      .filter(hasUrlPatterns($"url", lit(Array(".*index.*"))))
      .take(3)(1)(0)

    assert(base1.toString == expected1)
    assert(base2.toString == expected2)
  }

  test("Has Languages") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val expected = "de"
    val base = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .select(detectLanguage(removeHTML($"content")).as("language"))
      .filter(
        hasLanguages(
          detectLanguage(removeHTML($"content")),
          lit(Array("de", "ht"))
        )
      )
      .take(1)(0)(0)

    assert(base.toString == expected)
  }

  test("Has Images") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val expected = "image/jpeg"
    val base = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .select($"mime_type_tika")
      .filter(hasImages($"crawl_date", detectMimeTypeTika($"bytes")))
      .take(1)(0)(0)

    assert(base.toString == expected)
  }

  test("Has Date") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val date = Array("2008.*")
    val base = RecordLoader
      .loadArchives(arcPath, sc)
      .all()
      .select($"crawl_date")
      .filter(hasDate($"crawl_date", lit(date)))
      .count()

    assert(base == 261)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
