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
// scalastyle:off underscore.import
import io.archivesunleashed.matchbox._
// scalastyle:on underscore.import
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class CountableRDDTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-spark"
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    conf.set("spark.driver.allowMultipleContexts", "true");
    sc = new SparkContext(conf)
  }

  test("count records") {
    val base = RecordLoader.loadArchives(arcPath, sc)
      .keepValidPages()
      .map(r => ExtractDomain(r.getUrl))
    val r = base
      .map(r => (r, 1))
      .reduceByKey(_ + _)
      .map(_.swap)
      .sortByKey(ascending = false)
      .map(_.swap)
      .collect()
    val r2 = base.countItems().collect()
    assert(r sameElements r2)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
