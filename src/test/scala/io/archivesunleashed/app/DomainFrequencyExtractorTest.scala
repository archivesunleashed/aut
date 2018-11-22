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
class DomainFrequencyExtractorTest extends FunSuite with BeforeAndAfter {
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

  test("DomainFrequencyExtractor") {
    val rdd = RecordLoader.loadArchives(arcPath, sc).keepValidPages()
    val df = RecordLoader.loadArchives(arcPath, sc).extractValidPagesDF()

    val dfResults = DomainFrequencyExtractor(df).collect()
    val rddResults = DomainFrequencyExtractor(rdd).collect()

    // Results should be:
    // +------------------+-----+
    // |            Domain|count|
    // +------------------+-----+
    // |   www.archive.org|  132|
    // |     deadlists.com|    2|
    // |www.hideout.com.br|    1|
    // +------------------+-----+

    assert(dfResults(0).get(0) == "www.archive.org")
    assert(dfResults(0).get(1) == 132)
    assert(dfResults(1).get(0) == "deadlists.com")
    assert(dfResults(1).get(1) == 2)
    assert(dfResults(2).get(0) == "www.hideout.com.br")
    assert(dfResults(2).get(1) == 1)

    assert(rddResults(0)._1 == "www.archive.org")
    assert(rddResults(0)._2 == 132)
    assert(rddResults(1)._1 == "deadlists.com")
    assert(rddResults(1)._2 == 2)
    assert(rddResults(2)._1 == "www.hideout.com.br")
    assert(rddResults(2)._2 == 1)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
