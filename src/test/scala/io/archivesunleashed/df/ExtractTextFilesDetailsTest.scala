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
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class TextFilesTest extends FunSuite with BeforeAndAfter with Matchers {
  private val warcPath = Resources.getResource("warc/example.txt.warc.gz").getPath
  private val testPath = Resources.getResource("warc/example.warc.gz").getPath
  private val filedescPath = Resources.getResource("arc/example.arc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-df"
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
  }

  test("Text Files DF extraction") {
    val df = RecordLoader.loadArchives(warcPath, sc)
      .textFiles()

    val extracted = df.select("url", "filename", "extension",
      "mime_type_web_server", "mime_type_tika", "md5")
      .orderBy(desc("md5")).head(2).toList
    assert(extracted.size == 1)
    assert("https://ruebot.net/files/aut-test-fixtures/aut-text.txt" == extracted(0)(0))
    assert("aut-text.txt" == extracted(0)(1))
    assert("txt" == extracted(0)(2))
    assert("text/plain" == extracted(0)(3))
    assert("application/gzip" == extracted(0)(4))
    assert("32abd404fb560ecf14b75611f3cc5c2c" == extracted(0)(5))
  }

  test("Text Files DF robots.txt") {
    val df = RecordLoader.loadArchives(testPath, sc)
      .textFiles()

    val robots = df.select("url").orderBy(desc("md5")).head(50).toList

    assert(robots.size == 4)
    robots(0)(0).toString should not include ("robots.txt")
    robots(1)(0).toString should not include ("robots.txt")
    robots(0)(0).toString should not include (".js")
    robots(1)(0).toString should not include (".js")
    robots(0)(0).toString should not include (".css")
    robots(1)(0).toString should not include (".css")
    robots(0)(0).toString should not include (".htm")
    robots(1)(0).toString should not include (".htm")
    robots(0)(0).toString should not include (".html")
    robots(1)(0).toString should not include (".html")
  }

  test("Text Files DF dns or filedesc") {
    val df = RecordLoader.loadArchives(filedescPath, sc)
      .textFiles()

    val issue362 = df.select("url").head(50).toList

    issue362(0)(0).toString should not include ("filedesc://")
    issue362(1)(0).toString should not include ("filedesc://")
    issue362(0)(0).toString should not include ("dns:")
    issue362(1)(0).toString should not include ("dns:")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
