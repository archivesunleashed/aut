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
class ExtractTextFilesDetailsTest extends FunSuite with BeforeAndAfter {
  private val warcPath = Resources.getResource("warc/example.txt.warc.gz").getPath
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
      .extractTextFilesDetailsDF()

    val extracted = df.select("url", "filename", "extension",
      "mime_type_web_server", "mime_type_tika", "md5")
      .orderBy(desc("md5")).head(1).toList
    assert(extracted.size == 1)
    assert("https://ruebot.net/files/aut-test-fixtures/aut-text.txt" == extracted(0)(0))
    assert("aut-text.txt" == extracted(0)(1))
    assert("txt" == extracted(0)(2))
    assert("text/plain" == extracted(0)(3))
    assert("application/gzip" == extracted(0)(4))
    assert("32abd404fb560ecf14b75611f3cc5c2c" == extracted(0)(5))
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
