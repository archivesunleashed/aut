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
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class WordProcessorFilesTest extends FunSuite with BeforeAndAfter {
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

  test("Word Processor DF extraction") {
    val df = RecordLoader.loadArchives(warcPath, sc)
      .wordProcessorFiles()

    val extracted = df.select("url", "filename", "extension",
      "mime_type_web_server", "mime_type_tika", "md5")
      .orderBy(desc("md5")).head(3).toList
    assert(extracted.size == 3)
    assert("https://ruebot.net/files/aut-test-fixtures/test-aut-fixtures.rtf" == extracted(0)(0))
    assert("test-aut-fixtures.rtf" == extracted(0)(1))
    assert("rtf" == extracted(0)(2))
    assert("application/rtf" == extracted(0)(3))
    assert("application/rtf" == extracted(0)(4))
    assert("e483512b65ba44d71e843c57de2adeb7" == extracted(0)(5))
    assert("https://ruebot.net/files/aut-test-fixtures/test-aut-fixtures.odt" == extracted(1)(0))
    assert("test-aut-fixtures.odt" == extracted(1)(1))
    assert("odt" == extracted(1)(2))
    assert("application/vnd.oasis.opendocument.text" == extracted(1)(3))
    assert("application/vnd.oasis.opendocument.text" == extracted(1)(4))
    assert("9ef1aaee5c18cd16c47e75aaa38bd393" == extracted(1)(5))
    assert("https://ruebot.net/files/aut-test-fixtures/test-aut-fixtures.docx" == extracted(2)(0))
    assert("test-aut-fixtures.docx" == extracted(2)(1))
    assert("docx" == extracted(2)(2))
    assert("application/vnd.openxmlformats-officedocument.wordprocessingml.document" == extracted(2)(3))
    assert("application/vnd.openxmlformats-officedocument.wordprocessingml.document" == extracted(2)(4))
    assert("51040165e60629c6bf63c2bd40b9e628" == extracted(2)(5))
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
