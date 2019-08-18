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
import org.apache.spark.sql.{DataFrame, Row}
// scalastyle:off underscore.import
import org.apache.spark.sql.functions._
// scalastyle:on underscore.import
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class GetExtensionMimeTest extends FunSuite with BeforeAndAfter {
  private val warcPath = Resources.getResource("warc/example.media.warc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-df"
  private var sc: SparkContext = _
  private var df: DataFrame = _
  private var extracted: List[Row] = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
  }

  test("Get extension of file from URL with no extension") {
    df = RecordLoader.loadArchives(warcPath, sc)
      .extractImageDetailsDF()

    extracted = df.select("url", "filename", "extension",
      "mime_type_web_server", "mime_type_tika", "md5")
      .orderBy(desc("md5")).head(3).toList
    assert(extracted.size == 1)
    assert("[https://ruebot.net/files/aut-test-fixtures/this_is_a_gif" == extracted(0)(0))
    assert("this_is_a_gif" == extracted(0)(1))
    assert("gif" == extracted(0)(2))
    assert("unknown" == extracted(0)(3))
    assert("image/gif" == extracted(0)(4))
    assert("d4e86ee767b93929bf7bd75da01394e3" == extracted(0)(5))
  }

  test("Get extension of file from URL with correct extension") {
    assert(extracted.size == 1)
    assert("[https://ruebot.net/files/aut-test-fixtures/real_png.png" == extracted(1)(0))
    assert("real_png.png" == extracted(1)(1))
    assert("png" == extracted(1)(2))
    assert("image/png" == extracted(1)(3))
    assert("image/png" == extracted(1)(4))
    assert("ced2b79ae0e55f741219ae42ebe297aa" == extracted(1)(5))
  }

  test("Get extension of file from URL with incorrect extension") {
    assert(extracted.size == 1)
    assert("[https://ruebot.net/files/aut-test-fixtures/this_is_a_jpeg.mp3" == extracted(2)(0))
    assert("this_is_a_jpeg.mp3" == extracted(2)(1))
    assert("jpg" == extracted(2)(2))
    assert("audio/mpeg" == extracted(2)(3))
    assert("image/jpeg" == extracted(2)(4))
    assert("70a4c9c1ab657782e6732b65224fd124" == extracted(2)(5))
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
