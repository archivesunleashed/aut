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
class JsonInformationExtractorTest extends FunSuite with BeforeAndAfter {
  private val arcPath =
    Resources.getResource("warc/example.pdf.warc.gz").getPath
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

  test("JSON information extractor DF") {
    val df = RecordLoader.loadArchives(arcPath, sc).json()
    val dfResults = JsonInformationExtractor(df).collect()
    val RESULTSLENGTH = 3

    assert(dfResults.length == RESULTSLENGTH)
    assert(dfResults(0).get(0) == "20190812222538")
    assert(dfResults(0).get(1) == "")
    assert(
      dfResults(0)
        .get(2) == "https://api.plu.mx/widget/other/artifact?type=doi&id=10.1109%2FJCDL.2019.00043&href=https%3A%2F%2Fplu.mx%2Fpitt%2Fa%2F%3Fdoi%3D10.1109%2FJCDL.2019.00043&ref=https%3A%2F%2Fyorkspace.library.yorku.ca%2Fxmlui%2Fhandle%2F10315%2F36158&pageToken=f74d46f3-f622-c670-e1bc-bdc3-aa500a283693&isElsWidget=false"
    )
    assert(dfResults(0).get(3) == "artifact")
    assert(dfResults(0).get(4) == "json")
    assert(dfResults(0).get(5) == "application/json")
    assert(dfResults(0).get(6) == "N/A")
    assert(dfResults(0).get(7) == "d41d8cd98f00b204e9800998ecf8427e")
    assert(dfResults(0).get(8) == "da39a3ee5e6b4b0d3255bfef95601890afd80709")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
