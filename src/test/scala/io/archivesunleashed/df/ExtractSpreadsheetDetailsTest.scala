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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class ExtractSpreadsheetDetailsTest extends FunSuite with BeforeAndAfter {
  private val warcPath = Resources.getResource("warc/example.docs.warc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-df"
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
  }

  test("Spreadsheet DF extraction") {
    val df = RecordLoader.loadArchives(warcPath, sc)
      .spreadsheets()

    val extracted = df.select("url", "filename", "extension",
      "mime_type_web_server", "mime_type_tika", "md5")
      .orderBy(desc("md5")).head(4).toList
    assert(extracted.size == 4)
    assert("https://ruebot.net/files/aut-test-fixtures/test-aut-fixture.xlsx" == extracted(0)(0))
    assert("test-aut-fixture.xlsx" == extracted(0)(1))
    assert("xlsx" == extracted(0)(2))
    assert("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" == extracted(0)(3))
    assert("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" == extracted(0)(4))
    assert("befb3304cb592e0761509bf626171071" == extracted(0)(5))
    assert("https://ruebot.net/files/aut-test-fixtures/test-aut-fixture%20-%20Sheet1.tsv" == extracted(1)(0))
    assert("test-aut-fixture%20-%20Sheet1.tsv" == extracted(1)(1))
    assert("tsv" == extracted(1)(2))
    assert("text/tab-separated-values" == extracted(1)(3))
    assert("text/plain" == extracted(1)(4))
    assert("8ce6e9489c1c1129cca0e3f1eb8206ce" == extracted(1)(5))
    assert("https://ruebot.net/files/aut-test-fixtures/test-aut-fixture.ods" == extracted(2)(0))
    assert("test-aut-fixture.ods" == extracted(2)(1))
    assert("ods" == extracted(2)(2))
    assert("application/vnd.oasis.opendocument.spreadsheet" == extracted(2)(3))
    assert("application/vnd.oasis.opendocument.spreadsheet" == extracted(2)(4))
    assert("7f70280757d8beb2d1bfd6fb1b6ae6e9" == extracted(2)(5))
    assert("https://ruebot.net/files/aut-test-fixtures/test-aut-fixture%20-%20Sheet1.csv" == extracted(3)(0))
    assert("test-aut-fixture%20-%20Sheet1.csv" == extracted(3)(1))
    assert("csv" == extracted(3)(2))
    assert("text/csv" == extracted(3)(3))
    assert("text/plain" == extracted(3)(4))
    assert("38c3a488b239ec7b9b8e377b78968ef5" == extracted(3)(5))

  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
