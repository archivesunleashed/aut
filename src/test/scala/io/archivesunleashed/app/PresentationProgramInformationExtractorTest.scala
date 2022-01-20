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
class PresentationProgramInformationExtractorTest
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

  test("Presentation program information extractor DF") {
    val df = RecordLoader.loadArchives(arcPath, sc).presentationProgramFiles()
    val dfResults = PresentationProgramInformationExtractor(df).collect()
    val RESULTSLENGTH = 2

    assert(dfResults.length == RESULTSLENGTH)
    assert(dfResults(0).get(0) == "20190815004338")
    assert(
      dfResults(0).get(
        1
      ) == "https://ruebot.net/files/aut-test-fixtures/aut-test-fixtures.pptx"
    )
    assert(dfResults(0).get(2) == "aut-test-fixtures.pptx")
    assert(dfResults(0).get(3) == "pptx")
    assert(
      dfResults(0).get(
        4
      ) == "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    )
    assert(
      dfResults(0).get(
        5
      ) == "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    )
    assert(dfResults(0).get(6) == "7a7b1fe4b6d311376eaced9de3b682ee")
    assert(dfResults(0).get(7) == "86fadca47b134b68247ccde62da4ce3f62b4d2ec")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
