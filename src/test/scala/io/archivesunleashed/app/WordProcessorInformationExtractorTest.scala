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
class WordProcessorInformationExtractorTest
    extends FunSuite
    with BeforeAndAfter {
  private val arcPath =
    Resources.getResource("warc/example.docs.warc.gz").getPath
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

  test("Word processor information extractor DF") {
    val df = RecordLoader.loadArchives(arcPath, sc).wordProcessorFiles()
    val dfResults = WordProcessorInformationExtractor(df).collect()
    val RESULTSLENGTH = 3

    assert(dfResults.length == RESULTSLENGTH)
    assert(
      dfResults(0).get(
        0
      ) == "https://ruebot.net/files/aut-test-fixtures/test-aut-fixtures.rtf"
    )
    assert(dfResults(0).get(1) == "test-aut-fixtures.rtf")
    assert(dfResults(0).get(2) == "rtf")
    assert(dfResults(0).get(3) == "application/rtf")
    assert(dfResults(0).get(4) == "application/rtf")
    assert(dfResults(0).get(5) == "e483512b65ba44d71e843c57de2adeb7")
    assert(dfResults(0).get(6) == "8cf3066421f0a07fcd6e7a3e86ebd447edf7cfcb")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
