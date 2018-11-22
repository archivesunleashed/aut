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
class ExtractImageDetailsTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-df"
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
  }

  test("Fetch image") {
    val df = RecordLoader.loadArchives(arcPath, sc)
      .extractImageDetailsDF()

    // We need this in order to use the $-notation
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on

    val extracted = df.select($"url", $"mime_type", $"width", $"height", $"md5")
      .orderBy(desc("md5")).head(2).toList
    assert(extracted.size == 2)
    assert("http://www.archive.org/images/mediatype_movies.gif" == extracted(0)(0))
    assert("image/gif" == extracted(0)(1))
    assert(21 == extracted(0)(2))
    assert(21 == extracted(0)(3))
    assert("http://www.archive.org/images/LOCLogoSmall.jpg" == extracted(1)(0))
    assert("image/jpeg" == extracted(1)(1))
    assert(275 == extracted(1)(2))
    assert(300 == extracted(1)(3))
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
