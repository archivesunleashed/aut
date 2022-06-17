/*
 * Copyright © 2017 The Archives Unleashed Project
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
class XmlInformationExtractorTest extends FunSuite with BeforeAndAfter {
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

  test("XML information extractor DF") {
    val df = RecordLoader.loadArchives(arcPath, sc).xml()
    val dfResults = XmlInformationExtractor(df).collect()
    val RESULTSLENGTH = 9

    assert(dfResults.length == RESULTSLENGTH)
    assert(dfResults(0).get(0) == "20080430204830")
    assert(
      dfResults(0)
        .get(1) == "http://www.archive.org/services/collection-rss.php"
    )
    assert(dfResults(0).get(2) == "collection-rss.php")
    assert(dfResults(0).get(3) == "xml")
    assert(dfResults(0).get(4) == "text/xml")
    assert(dfResults(0).get(5) == "application/rss+xml")
    assert(dfResults(0).get(6) == "647a665e6acc2141af6d377b02e16c99")
    assert(dfResults(0).get(7) == "4dee969d37e188ce705c6b99b8a6ca62aa1418e5")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
