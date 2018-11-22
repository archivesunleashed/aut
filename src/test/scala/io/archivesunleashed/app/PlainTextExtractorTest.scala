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

package io.archivesunleashed.app

import com.google.common.io.Resources
import io.archivesunleashed.RecordLoader
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class PlainTextExtractorTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("warc/example.warc.gz").getPath
  private var sc: SparkContext = _
  private val master = "local[4]"
  private val appName = "example-spark"

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    conf.set("spark.driver.allowMultipleContexts", "true")
    sc = new SparkContext(conf)
  }

  test("PlainTextExtractorTest") {
    val rdd = RecordLoader.loadArchives(arcPath, sc).keepValidPages()
    val df = RecordLoader.loadArchives(arcPath, sc).extractValidPagesDF()
    val rddResults = PlainTextExtractor(rdd).collect()
    val dfResults = PlainTextExtractor(df).collect()
    val RESULTSLENGTH = 135

    assert(rddResults.length == RESULTSLENGTH)
    assert(rddResults(0)._1 == "20080430")
    assert(rddResults(0)._2 == "www.archive.org")
    assert(rddResults(0)._3 == "http://www.archive.org/")
    assert(rddResults(0)._4 == "HTTP/1.1 200 OK Date: " +
      "Wed, 30 Apr 2008 20:48:25 GMT Server: Apache/2.0.54 (Ubuntu) " +
      "PHP/5.0.5-2ubuntu1.4 mod_ssl/2.0.54 OpenSSL/0.9.7g Last-Modified: " +
      "Wed, 09 Jan 2008 23:18:29 GMT ETag: \"47ac-16e-4f9e5b40\" " +
      "Accept-Ranges: bytes Content-Length: 366 Connection: " +
      "close Content-Type: text/html; charset=UTF-8 Please visit " +
      "our website at: http://www.archive.org")

    assert(dfResults.length == RESULTSLENGTH)
    assert(dfResults(0).get(0) == "20080430")
    assert(dfResults(0).get(1) == "www.archive.org")
    assert(dfResults(0).get(2) == "http://www.archive.org/")
    assert(dfResults(0).get(3) == "HTTP/1.1 200 OK Date: " +
      "Wed, 30 Apr 2008 20:48:25 GMT Server: Apache/2.0.54 (Ubuntu) " +
      "PHP/5.0.5-2ubuntu1.4 mod_ssl/2.0.54 OpenSSL/0.9.7g Last-Modified: " +
      "Wed, 09 Jan 2008 23:18:29 GMT ETag: \"47ac-16e-4f9e5b40\" " +
      "Accept-Ranges: bytes Content-Length: 366 Connection: " +
      "close Content-Type: text/html; charset=UTF-8 Please visit " +
      "our website at: http://www.archive.org")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
