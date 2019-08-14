/*
 * Archives Unleashed Toolkit (AUT):
 * An open-source toolkit for analyzing web archives.
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
// scalastyle:off underscore.import
import io.archivesunleashed.df._
import org.apache.spark.sql.functions._
// scalastyle:on underscore.import
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class ExtractPresentationProgramDetailsTest extends FunSuite with BeforeAndAfter {
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
      .extractPresentationProgramDetailsDF()

    val extracted = df.select("url", "filename", "extension",
      "mime_type_web_server", "mime_type_tika", "md5")
      .orderBy(desc("md5")).head(2).toList
    assert(extracted.size == 2)
    assert("https://ruebot.net/files/aut-test-fixtures/aut-test-fixtures.odp" == extracted(0)(0))
    assert("aut-test-fixtures.odp" == extracted(0)(1))
    assert("odp" == extracted(0)(2))
    assert("application/vnd.oasis.opendocument.presentation" == extracted(0)(3))
    assert("application/vnd.oasis.opendocument.presentation" == extracted(0)(4))
    assert("f38b2679029cf3453c8151b92c615c70" == extracted(0)(5))
    assert("https://ruebot.net/files/aut-test-fixtures/aut-test-fixtures.pptx" == extracted(1)(0))
    assert("aut-test-fixtures.pptx" == extracted(1)(1))
    assert("pptx" == extracted(1)(2))
    assert("application/vnd.openxmlformats-officedocument.presentationml.presentation" == extracted(1)(3))
    assert("application/vnd.openxmlformats-officedocument.presentationml.presentation" == extracted(1)(4))
    assert("7a7b1fe4b6d311376eaced9de3b682ee" == extracted(1)(5))
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
