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
class ImageInformationExtractorTest extends FunSuite with BeforeAndAfter {
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

  test("Image information extractor DF") {
    val df = RecordLoader.loadArchives(arcPath, sc).images()
    val dfResults = ImageInformationExtractor(df).collect()
    val RESULTSLENGTH = 55

    assert(dfResults.length == RESULTSLENGTH)
    assert(dfResults(0).get(0) == "http://www.archive.org/images/logoc.jpg")
    assert(dfResults(0).get(1) == "logoc.jpg")
    assert(dfResults(0).get(2) == "jpg")
    assert(dfResults(0).get(3) == "image/jpeg")
    assert(dfResults(0).get(4) == "image/jpeg")
    assert(dfResults(0).get(5) == 70)
    assert(dfResults(0).get(6) == 56)
    assert(dfResults(0).get(7) == "8211d1fbb9b03d8522a1ae378f9d1b24")
    assert(dfResults(0).get(8) == "a671e68fc211ee4996a91e99297f246b2c5faa1a")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
