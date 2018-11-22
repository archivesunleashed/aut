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
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class WarcTest extends FunSuite with BeforeAndAfter {

  private val warcPath = Resources.getResource("warc/example.warc.gz").getPath
  private val master = "local[2]"
  private val appName = "example-spark"
  private var sc: SparkContext = _
  private var records: RDD[ArchiveRecord] = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    conf.set("spark.driver.allowMultipleContexts", "true");
    sc = new SparkContext(conf)
    records = RecordLoader.loadArchives(warcPath, sc)
  }

  test("count records") {
    assert(299L == records.count)
  }

  test("warc extract domain") {
    val take = 10
    val expectedLength = 3
    val r = records
      .keepValidPages()
      .map(r => r.getDomain)
      .countItems()
      .take(take)

    assert(r.length == expectedLength)
  }

  test("warc get content") {
    val a = RecordLoader.loadArchives(warcPath, sc)
      .map(r => r.getContentString)
      .take(1)
    assert(a.head.nonEmpty)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
