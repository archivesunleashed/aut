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
package io.archivesunleashed.df

import io.archivesunleashed.DataFrameLoader
import com.google.common.io.Resources
// scalastyle:off underscore.import
import org.apache.spark.sql.functions._
// scalastyle:on underscore.import
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class DataFrameLoaderTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-df"
  private var sc: SparkContext = _
  private val url = "url"
  private val mime_type = "mime_type_web_server"
  private val md5 = "md5"

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
  }

  test("Test DataFrameLoader") {
    val df = new DataFrameLoader(sc)
    val validPages = df.extractValidPages(arcPath)
    val hyperlinks = df.extractHyperlinks(arcPath)
    val imageLinks = df.extractImageLinks(arcPath)
    val images = df.extractImages(arcPath)

    val r_1 = validPages.select(url, mime_type).take(1)(0)
    assert(r_1.getAs[String](url) == "http://www.archive.org/")
    assert(r_1.getAs[String](mime_type) == "text/html")

    val r_2 = hyperlinks.select("Dest", "Anchor").take(3)(2)
    assert(r_2(0) == "http://web.archive.org/collections/web/advanced.html")
    assert(r_2(1) == "Advanced Search")

    val r_3 = imageLinks.take(100)(99)
    assert(r_3.get(0) == "http://www.archive.org/details/secretarmiesb00spivrich")
    assert(r_3.get(1) == "http://www.archive.org/images/star.png")

    val r_4 = images.take(1)(0)
    assert(r_4.getAs[String](url) == "http://www.archive.org/images/logoc.jpg")
    assert(r_4.getAs[String](md5) == "8211d1fbb9b03d8522a1ae378f9d1b24")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
