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
class HtmlInformationExtractorTest extends FunSuite with BeforeAndAfter {
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

  test("HTML information extractor DF") {
    val df = RecordLoader.loadArchives(arcPath, sc).html()
    val dfResults = HtmlInformationExtractor(df).collect()
    val RESULTSLENGTH = 140

    assert(dfResults.length == RESULTSLENGTH)
    assert(dfResults(0).get(0) == "20080430204826")
    assert(dfResults(0).get(1) == "20080109231829")
    assert(dfResults(0).get(2) == "http://www.archive.org/")
    assert(dfResults(0).get(3) == "")
    assert(dfResults(0).get(4) == "html")
    assert(dfResults(0).get(5) == "text/html")
    assert(dfResults(0).get(6) == "text/html")
    assert(dfResults(0).get(7) == "990fc5f1674fd21b9a035cf9193c3f10")
    assert(dfResults(0).get(8) == "d5817bf5b4b35a296823509dd754700a6ad522b5")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
