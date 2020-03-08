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

import io.archivesunleashed.df.{DetectLanguageDF, ExtractDomainDF, RemoveHTMLDF,
                                hasContent, hasDate, hasDomains, hasHttpStatus, 
                                hasLanguages, hasMimeTypes, hasUrlPatterns, hasUrls}
import com.google.common.io.Resources
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.sql.functions.lit

@RunWith(classOf[JUnitRunner])
class RecordDFTest extends FunSuite with BeforeAndAfter{
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

  test("hasHttpStatus") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val expected = "000"
    val base = RecordLoader.loadArchives(arcPath, sc)
      .all()
      .select($"http_status_code")
      .filter(hasHttpStatus($"http_status_code", lit(Array("200","000"))))
      .take(1)(0)(0) 

    assert (base.toString == expected)
  }

  test("hasUrls") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val expected1 = "http://www.archive.org/robots.txt"
    val expected2 = "http://www.archive.org/"
    val base1 = RecordLoader.loadArchives(arcPath, sc)
                            .all()
                            .select($"url")
                            .filter(hasUrls($"url", lit(Array("http://www.archive.org/","http://www.archive.org/robots.txt"))))
                            .take(1)(0)(0)

    val base2 = RecordLoader.loadArchives(arcPath, sc)
                            .all()
                            .select($"url")
                            .filter(hasUrls($"url", lit(Array("http://www.archive.org/"))))
                            .take(1)(0)(0)

    assert (base1.toString == expected1)
    assert (base2.toString == expected2)
  }

  test("hasDomains") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val expected = "http://www.archive.org/robots.txt"
    val base1 = RecordLoader.loadArchives(arcPath, sc)
                            .all()
                            .select($"url")
                            .filter(hasDomains(ExtractDomainDF($"url"), lit(Array("www.archive.org"))))
                            .take(1)(0)(0)

    assert (base1.toString == expected)
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

  test("hasMimeTypes") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val expected = "text/html"
    val base = RecordLoader.loadArchives(arcPath, sc)
                           .all()
                           .select($"mime_type_web_server")
                           .filter(hasMimeTypes($"mime_type_web_server", lit(Array("text/html"))))
                           .take(1)(0)(0)

    assert (base.toString == expected)
  }

  test("hasContent") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val expected = "http://www.archive.org/images/logoc.jpg"
    val base = RecordLoader.loadArchives(arcPath, sc)
                           .all()
                           .select($"url",$"content")
                           .filter(hasContent($"content", lit(Array("Content-Length: [0-9]{4}"))))
                           .take(1)(0)(0)

    assert (base.toString == expected)
  }

  test("hasUrlPatterns") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val expected1 = "http://www.archive.org/images/go-button-gateway.gif"
    val base1 = RecordLoader.loadArchives(arcPath, sc)
                            .all()
                            .select($"url")
                            .filter(hasUrlPatterns($"url", lit(Array(".*images.*"))))
                            .take(2)(1)(0)

    val expected2 = "http://www.archive.org/index.php?skin=classic"
    val base2 = RecordLoader.loadArchives(arcPath, sc)
                            .all()
                            .select($"url")
                            .filter(hasUrlPatterns($"url", lit(Array(".*index.*"))))
                            .take(3)(1)(0)

    assert (base1.toString == expected1)
    assert (base2.toString == expected2)
  }

  test("hasLanguages") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val expected = "de"
    val base = RecordLoader.loadArchives(arcPath, sc)
                           .all()
                           .select(DetectLanguageDF(RemoveHTMLDF($"content")).as("language"))
                           .filter(hasLanguages(DetectLanguageDF(RemoveHTMLDF($"content")), lit(Array("de","ht"))))
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
