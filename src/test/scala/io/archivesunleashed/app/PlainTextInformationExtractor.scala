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
class PlainTextInformationExtractorTest extends FunSuite with BeforeAndAfter {
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

  test("Plain text information extractor DF") {
    val df = RecordLoader.loadArchives(arcPath, sc).plainText()
    val dfResults = PlainTextInformationExtractor(df).collect()
    val RESULTSLENGTH = 2

    assert(dfResults.length == RESULTSLENGTH)
    assert(dfResults(0).get(0) == "20080430205210")
    assert(dfResults(0).get(1) == "20060210060725")
    assert(
      dfResults(0).get(2) == "http://www.archive.org/includes/csshover.htc"
    )
    assert(dfResults(0).get(3) == "csshover.htc")
    assert(dfResults(0).get(4) == "htc")
    assert(dfResults(0).get(5) == "text/plain")
    assert(dfResults(0).get(6) == "text/plain")
    assert(dfResults(0).get(7) == "b177c2210b9b88ba772137c129f361ce")
    assert(dfResults(0).get(8) == "f6d6e109017a2d3caa96f2c18564a77344fc048a")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
