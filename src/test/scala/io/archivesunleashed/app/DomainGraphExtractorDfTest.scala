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
class DomainGraphExtractorDfTest extends FunSuite with BeforeAndAfter {
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

  test("DomainGraphExtractor") {
    val TESTLENGTH = 166
    val TESTRESULT = 316
    val df = RecordLoader.loadArchives(arcPath, sc).extractHyperlinksDF()
    val dfResult = DomainGraphExtractor(df).collect()
    assert(dfResult.length == TESTLENGTH)
    assert(dfResult(0).get(0) == "20080430")
    assert(dfResult(0).get(1) == "archive.org")
    assert(dfResult(0).get(2) == "archive.org")
    assert(dfResult(0).get(3) == TESTRESULT)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
