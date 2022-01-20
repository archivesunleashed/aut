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
class WebGraphExtractorTest extends FunSuite with BeforeAndAfter {
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

  test("Web graph extractor DF") {
    val df = RecordLoader.loadArchives(arcPath, sc).webgraph()
    val dfResults = WebGraphExtractor(df).collect()
    val RESULTSLENGTH = 37826

    assert(dfResults.length == RESULTSLENGTH)
    assert(dfResults(0).get(0) == "20080430204826")
    assert(dfResults(0).get(1) == "http://www.archive.org/")
    assert(dfResults(0).get(2) == "http://www.archive.org")
    assert(dfResults(0).get(3) == "http://www.archive.org")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
