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
class JsInformationExtractorTest extends FunSuite with BeforeAndAfter {
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

  test("JS information extractor DF") {
    val df = RecordLoader.loadArchives(arcPath, sc).js()
    val dfResults = JsInformationExtractor(df).collect()
    val RESULTSLENGTH = 8

    assert(dfResults.length == RESULTSLENGTH)
    assert(dfResults(0).get(0) == "20080430204833")
    assert(dfResults(0).get(1) == "http://www.archive.org/flv/flv.js?v=1.34")
    assert(dfResults(0).get(2) == "flv.js")
    assert(dfResults(0).get(3) == "js")
    assert(dfResults(0).get(4) == "application/x-javascript")
    assert(dfResults(0).get(5) == "text/plain")
    assert(dfResults(0).get(6) == "8c73985a47e0d3720765d92fbde8cc9f")
    assert(dfResults(0).get(7) == "83a0951127abb1da11b141ad22ac72c20f2b4804")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
